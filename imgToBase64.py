# coding=utf-8

import sys
import base64




def toBase64(imgPath):
    base64Data = ""
    with open(imgPath, "rb") as img:
        base64Data = base64.b64encode(img.read())
    with open(imgPath+".tmp", "wb") as imgBase64:
        imgBase64.write(base64Data)
    return base64Data


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python " + sys.argv[0] + " IMG_PATH"

    imgPath = sys.argv[1]
    toBase64(imgPath)
    print "The base64 data of the img was written in " + imgPath + ".tmp"
