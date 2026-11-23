#!/bin/bash
BEELINE=$(command -v beeline || echo /opt/hive/bin/beeline)

echo "==> 等待 HiveServer2 就绪（最多约 5 分钟）"
for i in $(seq 1 60); do
  if "$BEELINE" -u jdbc:hive2://hive-server:10000 -e "show databases;" >/dev/null 2>&1; then
    echo "==> HiveServer2 已就绪（第 $i 次探测）"
    break
  fi
  echo "    等待 HiveServer2 ... ($i/60)"
  sleep 5
done

echo "==> 执行 01_orders_raw.sql：创建 HDFS 外部表 ods.orders_raw"
"$BEELINE" -u jdbc:hive2://hive-server:10000 -f /hive/init/01_orders_raw.sql \
  || echo "[WARN] 01_orders_raw.sql 执行失败，请查阅本容器日志"

echo "==> 执行 02_orders_hbase.sql：创建 HBase 映射表 ods.orders_hbase"
"$BEELINE" -u jdbc:hive2://hive-server:10000 -f /hive/init/02_orders_hbase.sql \
  || echo "[WARN] 02_orders_hbase.sql 执行失败（多半是缺 hbase-client jar）"

JDBC="jdbc:hive2://hive-server:10000"
# 不能用 hdfs dfs 探测：bde2020/hive 镜像的 core-site.xml 未配置 fs.defaultFS，
# hdfs 命令会退化成查容器本地文件系统，永远看不到 HDFS 上的分区。
# 改用 WebHDFS REST 接口，只依赖 namenode:9870 可达。
WEBHDFS="http://namenode:9870/webhdfs/v1/data/orders?op=LISTSTATUS"

echo "==> 等待 HDFS 出现 dt=* 分区（最多 10 分钟）"
for i in $(seq 1 60); do
  PART=$(curl -s --max-time 5 "$WEBHDFS" 2>/dev/null | grep -o '"pathSuffix":"dt=' | wc -l)
  if [ "${PART:-0}" -gt 0 ]; then
    echo "==> 检测到 ${PART} 个分区目录，开始注册分区"; break
  fi
  echo "    等待 logstash 写入 HDFS ... ($i/60)"; sleep 10
done

echo "==> 执行 MSCK REPAIR TABLE ods.orders_raw"
"$BEELINE" -u "$JDBC" -e "MSCK REPAIR TABLE ods.orders_raw;" || echo "[WARN] MSCK 失败"

echo "==> 校验 orders_raw 确实有数据（最多 5 分钟）"
for i in $(seq 1 30); do
  N=$("$BEELINE" -u "$JDBC" --outputformat=tsv2 \
      -e "select count(1) from ods.orders_raw;" 2>/dev/null | tail -1 | tr -d '[:space:]')
  if [ -n "$N" ] && [ "$N" -gt 0 ] 2>/dev/null; then
    echo "==> orders_raw 已有 ${N} 行，初始化完成"; break
  fi
  echo "    等待数据落地 ... ($i/30)"; sleep 10
done

echo "==> hive-init 完成，容器退出（Exit 0 属正常现象）"
