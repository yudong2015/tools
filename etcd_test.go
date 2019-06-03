package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.etcd.io/etcd/clientv3"
	"openpitrix.io/openpitrix/pkg/etcd"
)

const (
	KEY = "kkk"
	LeaseTimeoutSec = 5
	TestRegistTimeout = 20*time.Second
	TestGetInterval = 3*time.Second
)

//go协程之间没有父子关系（除main），即父协程停止了，子协程可以继续运行

func main(){
	cli1 := GetEtcd()
	defer cli1.Close()
	go run(cli1)

	cli2 := GetEtcd()
	defer cli2.Close()
	go watch(context.Background(), cli2, KEY)

	cli3 := GetEtcd()
	defer cli3.Close()
	geting(context.Background(), cli3, KEY, "GGG", 30*time.Second)
}

func run(cli *etcd.Etcd){
	ctx := context.Background()

	grantRes, err := cli.Lease.Grant(ctx, LeaseTimeoutSec)
	if err != nil {
		fmt.Printf("Error: %+v \n", err)
	}else {
		fmt.Printf("[%s]grantRes: %+v \n", time.Now().Local(), grantRes.ID)
	}

	_, err = cli.Put(ctx, KEY, "aaaa", clientv3.WithLease(grantRes.ID))
	if err != nil {
		fmt.Printf("Error: %+v \n", err)
	}else{
		fmt.Printf("[%s]Regist Put Done.\n", time.Now().Local())
	}

	go alive(ctx, cli, grantRes.ID)

	time.Sleep(TestRegistTimeout)
	fmt.Printf("[%s]Regist Done.. \n", time.Now().Local())

}

func GetEtcd() *etcd.Etcd {
	cli, err := etcd.Connect([]string{"127.0.0.1:2379"}, "test")
	if err != nil {
		fmt.Printf("Error: %+v", err)
		os.Exit(1)
	}
	return cli
}

func get(ctx context.Context, cli *etcd.Etcd, key string, index string){
	get, err := cli.Get(ctx, key)
	if err != nil {
		fmt.Printf("[%s][%s]Error: %+v \n", time.Now().Local(), index, err)
	}else{
		if get.Count > 0 {
			fmt.Printf("[%s][%s]Get: %s \n", time.Now().Local(), index, get.Kvs[0].Value)
		}else{
			fmt.Printf("[%s][%s]Get None.. \n", time.Now().Local(), index)
		}
	}
}

func geting(ctx context.Context, cli *etcd.Etcd, key string, index string, timeout time.Duration){
	now := time.Second
	for {
		if now > timeout {
			break
		}
		get(ctx, cli, key, index)
		time.Sleep(TestGetInterval)
		now += TestGetInterval
	}
}

func watch(ctx context.Context, cli *etcd.Etcd, KEY string){
	rch := cli.Watch(ctx, KEY)
	for res := range rch {
		for _, ev := range res.Events {
			fmt.Printf("[%s]Watch----> %s %q: %q \n", time.Now().Local(), ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func alive(ctx context.Context,cli *etcd.Etcd, ID clientv3.LeaseID){
	for {
		_, err := cli.Lease.KeepAliveOnce(ctx, ID)
		fmt.Printf("[%s]Regist KeepAliveOnce, err: %+v \n", time.Now().Local(), err)
		time.Sleep((LeaseTimeoutSec-1)*time.Second)
	}
}
