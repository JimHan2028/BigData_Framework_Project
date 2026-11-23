#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
电商订单离线批分析（Spark）

输入：HDFS /data/orders/dt=YYYY-MM-DD/orders-HH.csv（由 Logstash 从 Kafka 落盘，CSV 无表头）
输出：HDFS /data/spark/<指标名> 的 Parquet 结果 + 控制台指标打印

分析内容：
  1. 品类维度：订单数、GMV（仅 paid）、客单价、取消率
  2. 日期维度：每日订单量、GMV
  3. 小时维度：下单时段分布
  4. 用户维度：消费 Top10 用户
"""
import argparse
import json
import os
import sys
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)

WEBHDFS = "http://namenode:9870/webhdfs/v1"


def _list_status(path):
    url = "%s%s?op=LISTSTATUS" % (WEBHDFS, path)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))["FileStatuses"]["FileStatus"]


def _total_bytes(path="/data/orders"):
    """统计 /data/orders 下所有文件的总字节数（dt=xxx 是目录，目录自身 length 恒为 0，必须下钻一层）"""
    total = 0
    for entry in _list_status(path):
        if entry.get("type") == "DIRECTORY":
            if entry.get("childrenNum", 0) > 0:
                child_path = "%s/%s" % (path, entry["pathSuffix"])
                total += sum(f.get("length", 0) for f in _list_status(child_path))
        else:
            total += entry.get("length", 0)
    return total


def wait_for_data(path="/data/orders", max_wait=600, interval=15):
    """轮询 WebHDFS，等 Logstash 把订单落到 HDFS 再开跑（compose 起来后数据是异步到达的）。"""
    waited = 0
    while waited <= max_wait:
        try:
            total = _total_bytes(path)
            if total > 0:
                print("[Spark] HDFS 已就绪：%s 累计 %d 字节，等待 %ds"
                      % (path, total, waited), flush=True)
                return True
            print("[Spark] HDFS 目录已存在但暂无数据，继续等待（已等 %ds）" % waited, flush=True)
        except Exception as e:
            print("[Spark] 等待 HDFS 数据中（%ds）：%s" % (waited, e), flush=True)
        time.sleep(interval)
        waited += interval
    print("[Spark] 警告：等待 %ds 后 HDFS 仍无数据，继续执行（结果可能为空）" % max_wait, flush=True)
    return False

CSV_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_time", StringType(), True),
    StructField("status", StringType(), True),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="hdfs://namenode:9000/data/orders")
    ap.add_argument("--output", default="hdfs://namenode:9000/data/spark")
    ap.add_argument("--wait-minutes", type=int, default=10,
                    help="等待 HDFS 出现订单数据的最大分钟数")
    args = ap.parse_args()

    wait_for_data("/data/orders", max_wait=args.wait_minutes * 60)

    spark = (SparkSession.builder
             .appName("ecommerce-orders-batch-analysis")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70, flush=True)
    print("[Spark] 读取 HDFS: %s" % args.input, flush=True)
    print("=" * 70, flush=True)

    # basePath 让 Spark 从 dt=xxx 目录名自动推断分区列
    df = (spark.read
          .option("basePath", args.input)
          .schema(CSV_SCHEMA)
          .csv(args.input)
          .withColumn("order_time", F.to_timestamp("order_time"))
          .withColumn("hour", F.hour("order_time")))

    total = df.count()
    print("[Spark] 总订单数: %d" % total, flush=True)
    if total == 0:
        print("[Spark] 警告：HDFS 无数据，请确认 logstash / orders-gen 已产生数据", flush=True)
        spark.stop()
        sys.exit(0)

    df.cache()

    paid = F.col("status") == "paid"
    cancelled = F.col("status") == "cancelled"

    # ---------- 1. 品类维度 ----------
    by_cat = (df.groupBy("category").agg(
        F.count("*").alias("order_cnt"),
        F.sum(F.when(paid, F.col("amount")).otherwise(0.0)).alias("gmv"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(cancelled, 1).otherwise(0)).alias("cancel_cnt"),
        F.round(F.sum(F.when(cancelled, 1).otherwise(0)) * 100.0 / F.count("*"), 2).alias("cancel_rate_pct"),
    ).orderBy(F.desc("gmv")))

    print("\n---------- 1) 各品类销售概况（按 GMV 降序）----------", flush=True)
    by_cat.show(20, truncate=False)

    # ---------- 2. 日期维度 ----------
    by_day = (df.groupBy("dt").agg(
        F.count("*").alias("order_cnt"),
        F.sum(F.when(paid, F.col("amount")).otherwise(0.0)).alias("gmv"),
        F.countDistinct("user_id").alias("uv"),
    ).orderBy("dt"))

    print("\n---------- 2) 每日订单与 GMV ----------", flush=True)
    by_day.show(50, truncate=False)

    # ---------- 3. 小时维度 ----------
    by_hour = (df.groupBy("hour").agg(
        F.count("*").alias("order_cnt"),
        F.round(F.sum("amount"), 2).alias("amount_sum"),
    ).orderBy("hour"))

    print("\n---------- 3) 下单时段分布（0-23 时）----------", flush=True)
    by_hour.show(24, truncate=False)

    # ---------- 4. 用户 Top10 ----------
    top_users = (df.filter(paid)
                 .groupBy("user_id").agg(
                     F.count("*").alias("order_cnt"),
                     F.round(F.sum("amount"), 2).alias("total_amount"))
                 .orderBy(F.desc("total_amount")).limit(10))

    print("\n---------- 4) 消费金额 Top10 用户 ----------", flush=True)
    top_users.show(10, truncate=False)

    # ---------- 落盘 Parquet ----------
    out = {
        "category_stats": by_cat,
        "daily_stats": by_day,
        "hourly_stats": by_hour,
        "top_users": top_users,
    }
    for name, d in out.items():
        path = "%s/%s" % (args.output.rstrip("/"), name)
        d.write.mode("overwrite").parquet(path)
        print("[Spark] 已写出 Parquet: %s" % path, flush=True)

    print("\n[Spark] 批分析完成，结果目录: %s" % args.output, flush=True)
    hold = int(os.environ.get("SPARK_UI_HOLD_SECONDS", "0") or 0)
    if hold > 0:
        print("[Spark] Spark UI 保持开启 %d 秒，请尽快访问 http://localhost:4040/ 截图" % hold, flush=True)
        time.sleep(hold)
    spark.stop()


if __name__ == "__main__":
    main()
