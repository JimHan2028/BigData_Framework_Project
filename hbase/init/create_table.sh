#!/bin/bash
# 创建 HBase orders 表（手册未提供脚本，此处按手册交互命令等价还原）
set -e

echo "==> 等待 HBase Master 就绪"
for i in $(seq 1 30); do
  if echo "list" | hbase shell -n > /dev/null 2>&1; then
    echo "HBase 已就绪"
    break
  fi
  echo "等待中... ($i/30)"
  sleep 3
done

echo "==> 创建表 orders"
echo "create 'orders', 'info'" | hbase shell -n || echo "表已存在，跳过"

echo "==> 当前表列表"
echo "list" | hbase shell -n
