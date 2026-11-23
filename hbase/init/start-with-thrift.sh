#!/bin/bash
# HBase standalone 启动脚本：先起 master/regionserver，再起 Thrift(9090)
# 为什么要这个脚本：
#   镜像自带的 /run.sh 只执行 start-hbase.sh，不启动 Thrift Server，
#   而 hbase-sink 用 happybase 连的是 hbase:9090，9090 无人监听就会一直刷
#   Could not connect to ('hbase', 9090)。
#   另外镜像 entrypoint 是 `exec $@`（未加引号），字符串形式的 command 会被
#   二次分词，导致 bash -c 只拿到第一段就退出（容器 exit 0 无限重启）。
#   所以这里落成文件，command 只传两个无空格的参数。

start-hbase.sh

# 等 master 就绪；thrift 起太快会连不上 ZK 直接退出
sleep 20

nohup hbase thrift start -p 9090 > /opt/hbase-1.2.6/logs/thrift.log 2>&1 &

# 保持容器前台不退出
tail -f /opt/hbase-1.2.6/logs/*
