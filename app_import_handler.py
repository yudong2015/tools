'''
Created on 2020-07-20

@author: jackyu
'''

import os
import re

import context

import api.error_code as ErrorCodes
import api.error_msg as ErrorMsg
from api.common import (
    is_console_super_user,
)
from api.error import Error
from appcenter_deploy_handler import validate_appcenter_config_package
from common import return_error, return_success
from db.constants import (
    INDEXED_COLUMNS,
    TB_APP,
    TB_APP_APPROVED,
    TB_APP_VERSION,
    TB_APP_APPROVED_VERSION,
    TB_APP_VERSION_PRICE,
    TB_CLUSTER_COMMON,
    TB_APP_VERSION_IMAGE,
    TB_APP_VERSION_UPGRADE,
    TB_COMMON_ATTACHMENT,
    TB_APP_BUNDLE,
    TB_APP_IMPORT_SETTING,
    CF_COMMON_ATTACHMENT,
    KEYSPACE_USER_COMMON_ATTACHMENT,
)
from log.logger import logger

from resource_control.app import get_approved_apps
from resource_control.app_version import get_app_approved_versions
from resource_control.billing.interface import load_app_price
from request.consolidator.appcenter_deploy.constants import (
    PACKAGE_MUSTACHE_NAME,
    PACKAGE_USER_CONFIG_NAME,
    PACKAGE_REPLACE_POLICY_NAME,
    MUSTACHE_WHOLE_PATTERN,
    MUSTACHE_OPERATOR_REPLACE_PATTERN,
)
from utils.id_tool import get_uuid
from utils.global_conf import get_appcenter_conf, get_server_conf, get_ca
from utils.json import json_load, json_dump
from utils.misc import (
    read_file,
    exec_cmd,
    remove_dir,
)

TEMPORARY_PATH = "/tmp/migrate_appcenter2"
APPS_DIRNAME = "apps"
IMAGES_DIRNAME = "images"
METADATA_FILE = "meta_data.json"

SETTING_STATUS_ACTIVE = "active"
SETTING_STATUS_DISABLED = "disabled"
SUPPORT_CONTAINER_TYPES = ["lxc", "kvm"]

STATUS_IMPORTING = "importing"


def handle_import_app(req):
    sender = req["sender"]
    ret = check_import_permission(sender)
    if isinstance(ret, Error):
        return return_error(req, ret)

    set_default = req.get("set_default", "")
    owner = req.get("owner", sender["user_id"])
    source_path = req.get("source_path", "")
    override = req.get("override", True)
    instance_class = req.get("instance_class", "")
    volume_class = req.get("volume_class", "")
    container_type = req.get("container_type", "")
    replace = req.get("replace", [])
    image_only = req.get("image_only", False)
    console_id = sender["console_id"]

    if set_default:
        ret = create_import_setting(owner, console_id, override, source_path,
                                    instance_class, volume_class,
                                    container_type, replace)
        if ret is None:
            return return_error(req,
                                Error(ErrorCodes.INTERNAL_ERROR,
                                      ErrorMsg.ERR_CODE_MSG_INTERNAL_ERROR))

    app_versions = req.get("app_versions", {})
    if app_versions:
        importer = Importer(source_path, ctx=context.instance(),
                            override=override, owner=owner,
                            container_type=container_type)
        ret = importer.check()
        if isinstance(ret, Error):
            return return_error(req, ret)

        # rsync app files from source_path to local
        ret = importer.rsync_to_local()
        if isinstance(ret, Error):
            return return_error(req, ret)

        # read app info from local_path
        local_app_ids, local_version_ids = \
            importer.read_apps(with_metadata=True)

        # check app_versions exist local_path
        for app_version in app_versions:
            for app_id, versions in app_version.items():
                if app_id not in local_app_ids:
                    logger.error("The app[%s] not exist at source_path!",
                                 app_id)
                    return return_error(req, not_found_error(app_id))
                for version_id in versions:
                    if version_id not in local_version_ids:
                        logger.error("The app version[%s] not exist at"
                                     " source_path!", version_id)
                        return return_error(req, not_found_error(version_id))

    return return_success(req, {})


# describe app files(configuration, image..) at remote device(transit / disk)
def handle_describe_remote_apps(req):
    ret = check_import_permission(req["sender"])
    if isinstance(ret, Error):
        return return_error(req, ret)

    source_path = req.get("source_path", "")
    importer = Importer(source_path)
    ret = importer.check()
    if isinstance(ret, Error):
        return return_error(req, ret)

    # rsync app files from source_path to local
    ret = importer.rsync_to_local()
    if isinstance(ret, Error):
        return return_error(req, ret)

    # read app info from local_path
    app_ids, version_ids = importer.read_apps()

    # get exist apps and versions
    db_app_set = get_approved_apps(app_ids, ["status_time"])
    if isinstance(db_app_set, Error):
        return return_error(req, db_app_set)
    db_version_set = get_app_approved_versions(
        {"version_id": version_ids},
        columns=["status_time"])
    if isinstance(db_version_set, Error):
        return return_error(req, db_version_set)

    white_list = get_appcenter_conf().get("app_white_list", [])

    # format app_set
    for app_id, app in importer.app_map.items():
        if app_id in db_app_set:  # update import_time from DB
            app["import_time"] = db_app_set[app_id]["status_time"]
        if app_id not in white_list:  # update support from white_list
            app["support"] = 0

        app_id_dir = path_join(importer.local_path, app["app_dir"],
                               APPS_DIRNAME, app_id)
        app_info_file = path_join_addition_json(app_id_dir, app_id)
        app_info = read_json_file(app_info_file)
        if not app_info:
            return return_error(req, not_found_error(app_info_file))
        app["app_name"] = app_info["app_name"]

        # get icon of app
        icon_file = path_join_addition_json(app_id_dir, app_info["icon"])
        icon_info = read_json_file(icon_file)
        if not icon_info:
            return return_error(req, not_found_error(icon_file))
        app["icon"] = icon_info["attachment_content"]["raw"]

        for version in app["versions"]:
            version_id = version["version_id"]
            if version_id in db_version_set:  # update import_time from DB
                version["import_time"] = \
                    db_version_set[version_id]["status_time"]

            # check if the version is compatible with current platform
            version_file = path_join_addition_json(app_id_dir, version_id)
            version_info = read_json_file(version_file)
            if not version_info:
                return return_error(req, not_found_error(version_file))
            version["version_name"] = version_info["name"]

            attachment_file = path_join_addition_json(
                app_id_dir, version_info["resource_kit"])
            attachment_info = read_json_file(attachment_file)
            if not attachment_info:
                return return_error(req, not_found_error(attachment_file))
            ret = validate_appcenter_config_package(
                req["sender"],
                attachment_info["attachment_content"],
                ignore_image=True,
                ignore_links=True)
            if isinstance(ret, Error):
                logger.warning("The app version[%s] is not compatible with"
                               " current platform:[%s], ignore.",
                               version_id, ret.get_message())
                version["support"] = 0

        return return_success(req, importer.app_map.values())


def path_join(*args, **kwargs):
    return os.path.join(*args, **kwargs)


def path_join_addition_json(*args, **kwargs):
    return "{}.json".format(os.path.join(*args, **kwargs))


def ls_dir(*args, **kwargs):
    return os.listdir(*args, **kwargs)


def read_json_file(json_file):
    content = json_load(read_file(json_file))
    if content is None:
        logger.error("Failed to read json file[%s]!", json_file)
    return content


def check_import_permission(user):
    if not is_console_super_user(user):
        logger.error("The user[%s] not allowed to import apps"
                     "(only super user)!", user["user_id"])
        return Error(ErrorCodes.PERMISSION_DENIED)
    return 0


def not_found_error(resource):
    return Error(ErrorCodes.RESOURCE_NOT_FOUND,
                 ErrorMsg.ERR_MSG_RESOURCE_NOT_FOUND, resource)


def create_import_setting(owner, console_id, override, source_path,
                          instance_class='', volume_class='',
                          container_type='kvm', replace=None):
    ctx = context.instance()
    # get active settings
    settings = ctx.pg.get_by_filter(TB_APP_IMPORT_SETTING,
                                    {"status": SETTING_STATUS_ACTIVE},
                                    columns=["setting_id"])
    if not settings:
        logger.error("Get active setting failed!")
        return None

    # insert new setting
    replace_str = ''
    if replace:  # list: [{old: xx, new:xx}, ..]
        replace_str = json_dump(replace)
        if not replace_str:
            logger.error("Failed to dump replace content!")
            return None

    setting_id = get_uuid("appis-")
    setting_info = {
        "setting_id": setting_id,
        "owner": owner,
        "source_path": source_path,
        "console_id": console_id,
        "override": override,
        "instance_class": instance_class,
        "volume_class": volume_class,
        "container_type": container_type,
        "replace": replace_str,
        "status": SETTING_STATUS_ACTIVE,
    }

    if not ctx.pg.insert(TB_APP_IMPORT_SETTING, setting_info):
        logger.error("Insert newly created app_import_setting for [%s]"
                     " to db failed" % setting_id)
        return None

    # update active setting to status
    active_setting_ids = settings.keys()
    if active_setting_ids:
        ret = ctx.pg.update(TB_APP_IMPORT_SETTING, active_setting_ids,
                            {"status": SETTING_STATUS_DISABLED})
        if not ret:
            logger.error("Update active setting to disabled failed!")
            return None
    return setting_id


def check_mustache(node, key, standard):
    keys = []
    if "." in key:  # just support two levels, eg: volume.class
        keys = key.split(".")
        if keys[0] not in node or keys[1] not in node[keys[0]]:
            return
    else:
        if key not in node:
            return

    val = node[keys[0]][keys[1]] if keys else node[key]

    # not check if it's set in config.json(need to render mustache "{{")
    if isinstance(val, basestring) and not val.isdigit():
        return
    if int(val) not in standard:
        if keys:
            node[keys[0]][keys[1]] = standard[0]
        else:
            node[key] = standard[0]


def check_config(cfg, item_list):
    if "default" in cfg and int(cfg["default"]) not in item_list:
        cfg["default"] = item_list[0]
    if "range" in cfg:
        cfg["range"] = item_list
    if "resource_group" in cfg:
        new_resource_group = [v for v in cfg["resource_group"]
                              if v in item_list]
        for _ in range(len(new_resource_group),
                       len(cfg["resource_group"])):
            new_resource_group.append(item_list[0])
        cfg["resource_group"] = new_resource_group


def repair_mustache(mustache, instance_classes, volume_types, version_id):
    _mustache = mustache
    mustache = re.sub(MUSTACHE_WHOLE_PATTERN,
                      MUSTACHE_OPERATOR_REPLACE_PATTERN, mustache)
    mustache = json_load(mustache)
    if not mustache:
        return Error(ErrorCodes.INTERNAL_ERROR)
    if "nodes" not in mustache:
        return _mustache
    nodes = mustache["nodes"]
    if isinstance(nodes, list):
        for node in nodes:
            check_mustache(node, "instance_class", instance_classes)
            check_mustache(node, "volume.class", volume_types)
    else:
        check_mustache(nodes, "instance_class", instance_classes)
        check_mustache(nodes, "volume.class", volume_types)
    mustache = json_dump(mustache)
    mustache = re.sub('"{{', '  {{', mustache)
    mustache = re.sub('}}"', '}}  \r\n', mustache)
    logger.info("version: %s, repaired mustache", version_id)
    return mustache


def repair_config(config, instance_classes, volume_types, version_id):
    _config = config
    config = json_load(config)
    if not config:
        return Error(ErrorCodes.INTERNAL_ERROR)
    if "properties" not in config:
        return _config
    props = config["properties"]
    for prop in props:
        if prop["key"].strip() != "cluster":
            continue
        cluster_props = prop["properties"]
        for cluster_prop in cluster_props:
            if cluster_prop["key"].strip() == "name" \
                    or cluster_prop["key"].strip() == \
                    "resource_group" \
                    or cluster_prop["key"].strip() == \
                    "description" \
                    or cluster_prop["key"].strip() == \
                    "vxnet" \
                    or cluster_prop["key"].strip() == \
                    "external_service":
                continue
            if "properties" not in cluster_prop:
                continue
            role_props = cluster_prop["properties"]
            for role_prop in role_props:
                if role_prop["key"].strip() == "instance_class":
                    check_config(role_prop, instance_classes)
                elif role_prop["key"].strip() == "volume_class":
                    check_config(role_prop, volume_types)
    config = json_dump(config)
    logger.info("version: %s, repaired config", version_id)
    return config


class Importer(object):
    source_path = ""

    remote = ""
    remote_path = ""

    remote_host = ""
    remote_user = "root"

    is_remote = False
    local_path = ""

    # check dir format(if single meta_data.json):
    # 1. single_metadata: true
    #    multi-apps in one meta_data.json: [source_path]/meta_data.json
    #                                      [source_path]/apps/
    #                                      [source_path]/images/
    # 2. single_metadata: false
    #    multi-apps in multi-dir: [source_path]/mysql/meta_data.json ..
    #                            [source_path]/etcd/meta_data.json ..
    single_metadata = None

    # {app_id: {app_id: "app-xxx",
    #   versions: [{version_id: xxx}, ..]},
    # ...}
    app_map = {}

    metadata = {}  # {app_dir: metadata content}

    zone_id = None
    region_id = None

    def __init__(self, source_path, ctx=None, override=True, console_id="",
                 owner="", container_type="", instance_class="",
                 volume_class=""):
        self.source_path = source_path
        if ":" in source_path:
            # source_path likes: 139.198.4.176:/pitrix/migrate_appcenter2
            self.is_remote = True
            self.remote = self.source_path.split(":")[0]
            self.remote_path = self.source_path.split(":")[1]

            if "@" in self.remote:
                self.remote_host = self.remote.split("@")[1]
                self.remote_user = self.remote.split("@")[0]
            else:
                self.remote_host = self.remote
                self.remote = "{}@{}".format(self.remote_user,
                                             self.remote_host)

            self.local_path = path_join(TEMPORARY_PATH,
                                        self.remote_host,
                                        self.remote_path)
        else:
            # source_path likes: /pitrix/migrate_appcenter2
            self.local_path = self.source_path

        self.ctx = ctx
        self.override = override
        self.console_id = console_id
        self.owner = owner
        self.container_type = container_type
        self.instance_class = instance_class
        self.volume_class = volume_class
        self.global_urca = None

    # rsync app files without image file to local
    def rsync_to_local(self):
        if self.is_remote:
            ret = self.check_access_remote()
            if isinstance(ret, Error):
                return ret

            exclude = "*/images/*.lz4"
            if self.is_single_metadata():
                exclude = "images/*.lz4"

            ret = exec_cmd("rsync --exclude {} -rvth {} {} ".format(
                exclude, self.source_path, self.local_path))
            if ret is None or ret[0] != 0:
                logger.error("Rsync remote files to local failed: [%s]")
                remove_dir(self.local_path)  # clean tmp dir
                return Error(ErrorCodes.INTERNAL_ERROR,
                             ErrorMsg.ERR_MSG_COMMAND_FAILED,
                             "rsync from {}".format(self.source_path))
        return 0

    def read_apps(self, with_metadata=False):
        exist_app_ids = []
        exist_version_ids = []

        app_dirs = []
        if self.is_single_metadata():
            app_dirs.append("")
        else:
            for app_dir in ls_dir(self.local_path):
                app_dirs.append(app_dir)

        for app_dir in app_dirs:
            # exist app/version/ca ids at current app_home
            pwd_app_ids = []
            pwd_version_ids = []
            pwd_ca_ids = []

            app_home = path_join(self.local_path, app_dir)
            metadata_file = path_join(app_home, METADATA_FILE)
            metadata = read_json_file(metadata_file)
            if not metadata:
                return not_found_error(metadata_file)
            if with_metadata:
                self.metadata[app_dir] = metadata

            apps_path = path_join(app_home, APPS_DIRNAME)
            apps = ls_dir(apps_path)
            # load local disk files
            for app_id in apps:
                app_info = {"app_id": app_id, "support": 1, "home_name": app_dir}
                app_path = path_join(apps_path, app_id)
                app_info_path = path_join_addition_json(app_path, app_id)
                if not os.path.isfile(app_info_path):
                    logger.warning("[%s] is not file, skip...", app_info_path)
                    continue
                pwd_app_ids.append(app_id)

                json_files = ls_dir(app_path)
                app_info["versions"] = []
                for json_file in json_files:
                    if json_file.startswith("appv-"):
                        version_id = json_file.replace(".json", "")
                        pwd_version_ids.append(json_file.replace(".json", ""))
                        app_info["versions"].append({"version_id": version_id,
                                                     "support": 1})
                    if json_file.startswith("ca-"):
                        pwd_ca_ids.append(json_file.replace(".json", ""))
                self.app_map[app_id] = app_info

            # check metadata
            for app in metadata["apps"]:
                if app not in pwd_app_ids:
                    logger.error("app[%s] not in [%s]", app, apps_path)
                    return not_found_error(app)
            for version in metadata["versions"]:
                if version not in pwd_version_ids:
                    logger.error("version [%s] not in [%s]",
                                 version, apps_path)
                    return not_found_error(version)
            for ca in metadata["attachments"]:
                if ca not in pwd_ca_ids:
                    logger.error("ca [%s] not in [%s]", ca, apps_path)
                return not_found_error(ca)
            exist_app_ids.extend(pwd_app_ids)
            exist_version_ids.extend(pwd_version_ids)

        return exist_app_ids, exist_version_ids

    def check(self):
        if not self.source_path:
            return Error(ErrorCodes.INVALID_REQUEST_FORMAT,
                         ErrorMsg.ERR_MSG_MISSING_PARAMETER, "source_path")

        ret = self.check_access_remote()
        if isinstance(ret, Error):
            return ret

        if self.container_type and \
                self.container_type not in SUPPORT_CONTAINER_TYPES:
            logger.error("The container_type[%s] doesn't support.")
            return Error(ErrorCodes.CONFIG_VALIDATE_ERROR,
                         ErrorMsg.ERR_MSG_UNSUPPORTED_PARAMETER_VALUE,
                         "container_type", self.container_type)
        return 0

    # check if remote path can be accessed by global node.
    def check_access_remote(self, file_path=None):
        if self.is_remote:
            remote = "{}@{}".format(self.remote_user, self.remote_host)
            path = self.remote_path
            if file_path:
                path = path_join(path, file_path)

            # check if remote source path exist
            ret = exec_cmd("ssh {} test -e {}".format(remote, path))
            if ret is None or ret[0] != 0:
                logger.error("Access remote source path[%s] failed: [%s]",
                             self.source_path, ret[2])
                return Error(ErrorCodes.PERMISSION_DENIED,
                             ErrorMsg.ERR_MSG_RESOURCE_ACCESS_DENIED,
                             "{}:{}".format(remote, path))
        return 0

    def is_single_metadata(self):
        if self.single_metadata is None:
            if self.is_remote:
                ret = self.check_access_remote(METADATA_FILE)
                self.single_metadata = True if ret == 0 else False
            else:
                self.single_metadata = \
                    os.path.exists(path_join(self.local_path, METADATA_FILE))
        return self.single_metadata

    def import_apps(self, app_versions, override):
        # insert to db
        for app_id, versions in app_versions:
            app_id_dir = path_join(self.local_path,
                                   self.app_map[app_id]["home_name"],
                                   APPS_DIRNAME, app_id)
            app_info_path = path_join_addition_json(app_id_dir, app_id)
            app_info = read_json_file(app_info_path)
            if not app_info:
                return not_found_error(app_info_path)

            # import_app
            ret = self.import_app(app_id, app_info)
            if isinstance(ret, Error):
                return ret

            for verion_id in versions:
                # import versions
                ret = self.import_version(app_id, verion_id, app_id_dir)
                if isinstance(ret, Error):
                    return ret

            ret = self.import_version_upgrades(app_id, versions)
            if isinstance(ret, Error):
                return ret

            ret = self.import_app_bundle(app_id, app_id_dir)
            if isinstance(ret, Error):
                return ret
            # TODO: import images
        return 0

    def import_app(self, app_id, app_info):
        if not self.override:
            ret = self.ctx.pg.get_count(TB_APP, {"app_id": app_id})
            if ret is None:
                return Error(ErrorCodes.INTERNAL_ERROR)

            if ret > 0:
                logger.warning("Override is False and app_id "
                               "[%s] already exist in db, skip.", app_id)
                return 0

        ret = self.global_db_delete(TB_APP, {"app_id": app_id})
        if isinstance(ret, Error):
            return ret
        ret = self.global_db_delete(TB_APP_APPROVED, {"app_id": app_id})
        if isinstance(ret, Error):
            return ret
        logger.info("Cleanup [%s] success.", app_id)

        app_info["console_id"] = self.console_id
        app_info["status"] = STATUS_IMPORTING
        ret = self.global_db_insert(TB_APP_APPROVED, app_info)
        if isinstance(ret, Error):
            return ret

        app_info["owner"] = self.owner
        app_info["root_user_id"] = self.owner
        # app_info["status"] = "approved"
        del app_info["admin_id"]
        del app_info["supported_vxnet_version_one"]
        ret = self.global_db_insert(TB_APP, app_info)
        if isinstance(ret, Error):
            return ret
        logger.info("Insert [%s] successfully.", app_id)
        return ret

    def import_version(self, app_id, version_id, app_id_dir):
        if not self.override:
            if self.ctx.pg.get(TB_APP_VERSION, version_id, ["version_id"]):
                logger.info("override is False and version [%s] "
                            "already exist in db, skip" % version_id)
                return 0

        version_info_file = path_join_addition_json(app_id_dir, version_id)
        version_info = read_json_file(version_info_file)
        if not version_info:
            return not_found_error(version_info_file)

        ret = self.global_db_delete(TB_APP_VERSION,
                                    {"version_id": version_id})
        if isinstance(ret, Error):
            return ret
        ret = self.global_db_delete(TB_APP_APPROVED_VERSION,
                                    {"version_id": version_id})
        if isinstance(ret, Error):
            return ret
        logger.info("cleanup [%s] success", version_id)

        version_info["console_id"] = self.console_id
        version_info["status"] = STATUS_IMPORTING
        ret = self.global_db_insert(TB_APP_APPROVED_VERSION, version_info)
        if isinstance(ret, Error):
            return ret

        version_info["owner"] = self.owner
        version_info["root_user_id"] = self.owner
        # version_info["status"] = "released"
        ret = self.global_db_insert(TB_APP_VERSION, version_info)
        if isinstance(ret, Error):
            return ret

        ret = self.active_version_price(app_id, version_id)
        if isinstance(ret, Error):
            return ret

        ret = self.register_app_version_images(app_id, version_id)
        if isinstance(ret, Error):
            return ret

        ret = self.import_common_attachment(version_info["resource_kit"],
                                            app_id_dir)
        if isinstance(ret, Error):
            return ret
        logger.info("insert [%s] success" % version_id)
        return 0

    def active_version_price(self, app_id, version_id):
        # insert price
        columns = {
            "app_id": app_id,
            "version_id": version_id,
            "status": "active",
        }
        if -1 == self.ctx.billing_resource_pg.base_insert(
                TB_APP_VERSION_PRICE, columns):
            logger.error("Failed to insert [%s] into table[%s]!",
                         columns, TB_APP_VERSION_PRICE)
            return Error(ErrorCodes.INTERNAL_ERROR)
        ret = load_app_price([app_id])
        if not ret:
            return Error(ErrorCodes.INTERNAL_ERROR)
        return 0

    def register_app_version_images(self, app_id, version_id):
        if self.container_type:
            columns = {"hypervisor": self.container_type}
            if -1 == self.ctx.zone_pg.base_update(
                    TB_CLUSTER_COMMON, {"app_version": version_id},
                    columns=columns):
                logger.error("Failed to update [%s] in table[%s]!",
                             columns, TB_CLUSTER_COMMON)
                return Error(ErrorCodes.INTERNAL_ERROR)

        home_name = self.app_map[app_id]["home_name"]
        images = self.metadata[home_name]["app_info"][app_id][version_id]
        for image in images:
            image_info = {"version_id": version_id, "image_id": image,
                          "user_id": self.owner, "app_id": app_id,
                          "zone_id": self.get_zone_id()}
            if -1 == self.ctx.base_insert(TB_APP_VERSION_IMAGE, image_info):
                logger.error("Failed to insert [%s] into table[%s]!",
                             image_info, TB_APP_VERSION_IMAGE)
                return Error(ErrorCodes.INTERNAL_ERROR)

            if self.get_region_id():
                image_info["zone_id"] = self.get_region_id()
                if -1 == self.ctx.base_insert(TB_APP_VERSION_IMAGE, image_info):
                    logger.error("Failed to insert [%s] into table[%s]!",
                                 image_info, TB_APP_VERSION_IMAGE)
                    return Error(ErrorCodes.INTERNAL_ERROR)
        return 0

    def import_common_attachment(self, ca_id, app_id_dir):
        if not self.override and self.ctx.pg.get(TB_COMMON_ATTACHMENT, ca_id):
            logger.error("Override is False and attachment_id[%s] "
                         "already exist in db, skip", ca_id)
            return 0

        atta_info_file = path_join_addition_json(app_id_dir, ca_id)
        attachment = read_json_file(atta_info_file)
        if not attachment:
            return not_found_error(atta_info_file)

        ret = self.global_db_delete(TB_COMMON_ATTACHMENT,
                                    {"attachment_id": ca_id})
        if isinstance(ret, Error):
            return ret
        if -1 == self.get_urca().remove(CF_COMMON_ATTACHMENT, ca_id):
            logger.error("Failed to remove from [%s] with ca_id[%s]!",
                         CF_COMMON_ATTACHMENT, ca_id)
            return Error(ErrorCodes.INTERNAL_ERROR)
        logger.info("cleanup [%s] success", ca_id)

        attachment["console_id"] = self.console_id
        attachment["owner"] = self.owner
        attachment["root_user_id"] = self.owner
        attachment.pop("source")
        attachment_content = attachment.pop("attachment_content")
        if not isinstance(attachment_content, dict):
            attachment_content = {"raw": attachment_content}

        if PACKAGE_MUSTACHE_NAME in attachment_content:
            ret = self.replace_mustache(
                attachment_content[PACKAGE_MUSTACHE_NAME], ca_id)
            if isinstance(ret, Error):
                return ret
            attachment_content[PACKAGE_MUSTACHE_NAME] = ret
        ret = self.repair(attachment_content, ca_id,
                          instance_class=self.instance_class,
                          volume_class=self.volume_class)
        if isinstance(ret, Error):
            return ret
        if PACKAGE_REPLACE_POLICY_NAME in attachment_content:
            attachment_content.pop(PACKAGE_REPLACE_POLICY_NAME)

        if 0 != self.get_urca().insert(CF_COMMON_ATTACHMENT, ca_id,
                                       attachment_content):
            logger.error("Failed to save attachment[%s] into cassandra!", ca_id)
            return Error(ErrorCodes.INTERNAL_ERROR)
        ret = self.global_db_insert(TB_COMMON_ATTACHMENT, attachment)
        if isinstance(ret, Error):
            return ret
        logger.info("insert [%s] success", ca_id)
        return 0

    def replace_mustache(self, mustache, ca_id):
        _mustache = mustache
        mustache = re.sub(MUSTACHE_WHOLE_PATTERN,
                          MUSTACHE_OPERATOR_REPLACE_PATTERN, mustache)
        mustache = json_load(mustache)
        if not mustache:
            logger.error("Failed to load mustache[%s]!", ca_id)
            return Error(ErrorCodes.INTERNAL_ERROR)
        if "nodes" not in mustache:
            return _mustache
        nodes = mustache["nodes"]
        if isinstance(nodes, list):
            for node in nodes:
                node["container"]["zone"] = self.zone_id
                if self.container_type:
                    node["container"]["type"] = self.container_type
        else:
            nodes["container"]["zone"] = self.zone_id
            if self.container_type:
                nodes["container"]["type"] = self.container_type
        mustache = json_dump(mustache)
        mustache = re.sub('"\{\{', '  {{', mustache)
        mustache = re.sub('\}\}"', '}}  \r\n', mustache)
        return mustache

    def get_zone_id(self):
        if self.zone_id is None:
            server_conf = get_server_conf()
            self.zone_id = server_conf.get('common', {}).get('zone_id', '')
        return self.zone_id

    def get_region_id(self):
        if self.region_id is None:
            server_conf = get_server_conf()
            self.region_id = server_conf.get('common', {}).get('region_id', '')
        return self.region_id

    # import upgrade versons of current importing versions
    def import_version_upgrades(self, app_id, importing_versions):
        app_home_name = self.app_map[app_id]["home_name"]
        version_upgrades = self.metadata[app_home_name].get(
            "version_upgrades", {})
        columns = [INDEXED_COLUMNS[TB_APP_APPROVED_VERSION][0]]
        for upgrade_version_id, version_ids in version_upgrades.items():
            if upgrade_version_id not in importing_versions:
                continue
            if not self.ctx.pg.get(TB_APP_APPROVED_VERSION,
                                   upgrade_version_id, columns):
                logger.warning("The upgrade_version[%s] not exist, ignore.",
                               upgrade_version_id)
                continue

            for version_id in version_ids:
                if not self.ctx.pg.get(TB_APP_APPROVED_VERSION,
                                       version_id, columns):
                    logger.warning("The app_approved_version[%s] not exist,"
                                   " ignore.", version_id)
                    continue
                ret = self.active_upgrade_policy(version_id,
                                                 upgrade_version_id)
                if isinstance(ret, Error):
                    return ret
        return 0

    def active_upgrade_policy(self, version_id, upgrade_version_id):
        policy = {
            "version_id": version_id,
            "upgrade_version_id": upgrade_version_id,
        }
        ret = self.global_db_delete(TB_APP_VERSION_UPGRADE, policy)
        if isinstance(ret, Error):
            return ret

        policy["status"] = "active"
        ret = self.global_db_insert(TB_APP_VERSION_UPGRADE, policy)
        if isinstance(ret, Error):
            return ret
        return 0

    def global_db_delete(self, table, condition):
        if -1 == self.ctx.pg.base_delete(table, condition):
            logger.error("Failed to delete from table[%s] with "
                         "condition[%s]!", table, condition)
            return Error(ErrorCodes.INTERNAL_ERROR)
        return 0

    def global_db_insert(self, table, columns):
        if -1 == self.ctx.pg.base_insert(table, columns):
            logger.error("Failed to save [%s] into table [%s]",
                         columns, table)
            return Error(ErrorCodes.INTERNAL_ERROR)
        return 0

    def get_urca(self):
        if self.global_urca is None:
            self.global_urca = get_ca(KEYSPACE_USER_COMMON_ATTACHMENT)
        return self.global_urca

    def repair(self, attachment_content, version_id,
               instance_class=None, volume_class=None):
        if PACKAGE_MUSTACHE_NAME not in attachment_content or \
                PACKAGE_USER_CONFIG_NAME not in attachment_content:
            logger.warning("There is no mustache or config of "
                           "app_version[%s], will not repair", version_id)
            return 0

        conf = get_server_conf()
        zone_id = self.get_zone_id()
        if zone_id not in conf['common']['resource_limits']:
            zone_id = "default"
        if not instance_class:
            instance_class = conf['common']['resource_limits'][zone_id]. \
                get('valid_instance_classes', '')
        if not volume_class:
            volume_class = conf['common']['resource_limits'][zone_id]. \
                get('valid_volume_types', '')

        if not instance_class or not volume_class:
            logger.warning("version [%s] no instance_class or volume_class "
                           "will not repair", version_id)
            return 0
        instance_classes = instance_class.split(',')
        for index, _ in enumerate(instance_classes):
            instance_classes[index] = int(instance_classes[index])
        instance_classes.sort()
        volume_types = volume_class.split(',')
        for index, _ in enumerate(volume_types):
            volume_types[index] = int(volume_types[index])
        volume_types.sort()
        logger.info("repair app_version[%s], instance_classes: %s, "
                    "volume_types: %s", version_id, instance_classes,
                    volume_types)
        if PACKAGE_MUSTACHE_NAME in attachment_content:
            ret = repair_mustache(attachment_content[PACKAGE_MUSTACHE_NAME],
                                  instance_classes, volume_types, version_id)
            if isinstance(ret, Error):
                return ret
            attachment_content[PACKAGE_MUSTACHE_NAME] = ret
        if PACKAGE_USER_CONFIG_NAME in attachment_content:
            ret = repair_config(attachment_content[PACKAGE_USER_CONFIG_NAME],
                                instance_classes, volume_types, version_id)
            if isinstance(ret, Error):
                return ret
            attachment_content[PACKAGE_USER_CONFIG_NAME] = ret
        return 0

    def import_app_bundle(self, app_id, app_id_dir):
        bundle_file = path_join(app_id_dir, "bundle-{}.json".format(app_id))
        if not os.path.exists(bundle_file):
            logger.info("The app_bundle of app[%s] not exist, skip.", app_id)
            return 0

        if not self.override and self.ctx.pg.base_get(TB_APP_BUNDLE,
                                                      {"app_id": app_id}):
            logger.info("override is False and app_id[%s] already "
                        "exist in table[%s], skip", app_id, TB_APP_BUNDLE)
            return 0

        app_bundle_info = read_json_file(bundle_file)
        if not app_bundle_info:
            return Error(ErrorCodes.INTERNAL_ERROR)

        ret = self.global_db_delete(TB_APP_BUNDLE, {"app_id": app_id})
        if isinstance(ret, Error):
            return ret
        logger.info("cleanup [%s] app_bundle success.", app_id)
        ret = self.global_db_insert(TB_APP_BUNDLE, app_bundle_info)
        if isinstance(ret, Error):
            return ret
        logger.info("insert [%s] bundle success.", app_id)
        return 0
