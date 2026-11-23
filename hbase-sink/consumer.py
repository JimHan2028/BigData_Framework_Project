# -*- coding: utf-8 -*-


import json
import time

import happybase
from kafka import KafkaConsumer

# ---------------- 连接参数 ----------------
# 全部是 docker compose 里的服务名，容器网络内直接可解析
KAFKA_BOOTSTRAP = ["kafka:9092"]
TOPIC = "orders"
GROUP_ID = "hbase-sink"          # 消费组名；与 logstash-hdfs / logstash-es 不同组，三方各收一份全量

HBASE_HOST = "hbase"
HBASE_PORT = 9090                # 9090 是 HBase Thrift 端口，happybase 走的就是这个
HBASE_TABLE = "orders"
HBASE_CF = b"info"               # 列族名，HBase 侧用字节串

BATCH_SIZE = 100                 # 每 100 条做一次批量提交（一次 RPC 写多行，比逐条 put 快很多）
RETRY_INTERVAL = 5               # 重连间隔（秒）


def log(msg):
    """统一日志格式，docker compose logs 里好筛。"""
    print("[hbase-sink] %s" % msg, flush=True)


def wait_for_hbase():
    """等待 HBase Thrift 服务可连接，返回一个已打开的 happybase 连接。"""
    while True:
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)
            conn.open()
            log("已连接 HBase：%s:%s" % (HBASE_HOST, HBASE_PORT))
            return conn
        except Exception as exc:                      # 连接被拒、超时、Thrift 未监听都走这里
            log("连接 HBase 失败（%s），%s 秒后重试 ..." % (exc, RETRY_INTERVAL))
            time.sleep(RETRY_INTERVAL)


def ensure_table(conn):
    """
    建表（幂等）：表已存在就跳过，不存在才创建。
    HBase 表必须先建好列族，否则 put 会直接报错。
    """
    try:
        tables = [t.decode("utf-8") if isinstance(t, bytes) else t for t in conn.tables()]
    except Exception:
        tables = []

    if HBASE_TABLE in tables:
        log("表 %s 已存在，跳过建表" % HBASE_TABLE)
        return

    # max_versions=1：订单是快照型数据，只保留最新一版，省空间
    conn.create_table(HBASE_TABLE, {HBASE_CF: {"max_versions": 1}})
    log("表 %s 创建完成（列族 info）" % HBASE_TABLE)


def to_bytes(value):
    """把 Python 值统一转成 HBase 需要的字节串；浮点保留两位小数。"""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, float):
        return ("%.2f" % value).encode("utf-8")
    return str(value).encode("utf-8")


def build_row(order):
    """
    把一条订单 JSON 转成 (rowkey, {列名: 值})。
    行键用 order_id，天然唯一且能把同一订单的所有列聚在同一行。
    """
    order_id = order.get("order_id") or ""
    rowkey = to_bytes(order_id)

    price = float(order.get("price") or 0)
    quantity = int(order.get("quantity") or 0)
    amount = order.get("amount")
    if amount is None:                                # 上游没给 amount 时按单价×数量补算
        amount = round(price * quantity, 2)

    data = {
        b"info:user_id": to_bytes(order.get("user_id")),
        b"info:product_id": to_bytes(order.get("product_id")),
        b"info:category": to_bytes(order.get("category")),
        b"info:price": to_bytes(round(price, 2)),
        b"info:quantity": to_bytes(quantity),
        b"info:amount": to_bytes(round(float(amount), 2)),
        b"info:order_time": to_bytes(order.get("order_time")),
        b"info:status": to_bytes(order.get("status")),
    }
    return rowkey, data


def create_consumer():
    """创建 Kafka 消费者；Kafka 未就绪时同样循环重试。"""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",   # 首次启动从头消费，保证历史数据也能进 HBase
                enable_auto_commit=False,       # 关自动提交：写入成功后再手动提交，避免丢消息
                max_poll_records=BATCH_SIZE,    # 一次最多拉 BATCH_SIZE 条，配合批量写
                value_deserializer=None,         # 下面自己 json.loads，便于容错
                consumer_timeout_ms=-1,          # 不因为"暂时没数据"就退出
            )
            log("已连接 Kafka：%s，topic=%s，group=%s" % (KAFKA_BOOTSTRAP, TOPIC, GROUP_ID))
            return consumer
        except Exception as exc:
            log("连接 Kafka 失败（%s），%s 秒后重试 ..." % (exc, RETRY_INTERVAL))
            time.sleep(RETRY_INTERVAL)


def main():
    conn = None
    consumer = None
    total = 0          # 本次进程累计成功写入条数

    while True:
        try:
            # ---- 1. 确保 HBase 可连、表已存在 ----
            if conn is None:
                conn = wait_for_hbase()
            ensure_table(conn)

            # ---- 2. 取表句柄与 Kafka 消费者 ----
            table = conn.table(HBASE_TABLE)
            if consumer is None:
                consumer = create_consumer()

            batch = table.batch(batch_size=BATCH_SIZE)   # 批量写句柄，满 BATCH_SIZE 自动提交
            log("开始消费并写入 HBase ...")

            # ---- 3. 主消费循环 ----
            for message in consumer:
                raw = message.value
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        order = json.loads(raw.decode("utf-8"))
                    except Exception:
                        log("跳过一条无法解析的消息（offset=%s）" % message.offset)
                        continue
                elif isinstance(raw, dict):
                    order = raw
                else:
                    continue

                rowkey, data = build_row(order)
                if not rowkey:
                    log("跳过一条缺少 order_id 的消息（offset=%s）" % message.offset)
                    continue

                # 单条写入失败直接抛到外层，让外层统一重连后重试，保证不静默丢数据
                batch.put(rowkey, data)
                total += 1

                # 每满一批：提交到 HBase + 提交消费位点
                if total % BATCH_SIZE == 0:
                    batch.send()                      # 真正落库
                    consumer.commit()                 # 落库成功后才提交位点
                    if total % 100 == 0:
                        log("已写入 HBase 累计 %d 条" % total)

            # for 循环正常结束（理论上不会，consumer_timeout_ms=-1 是无限等待）
            log("消费循环结束，准备重连 ...")

        except KeyboardInterrupt:
            log("收到中断信号，退出（累计写入 %d 条）" % total)
            break
        except Exception as exc:
            # 这里捕获 HBase region 迁移、Kafka rebalance、网络抖动等所有异常
            log("处理异常：%s；%s 秒后重建连接继续消费 ..." % (exc, RETRY_INTERVAL))
            # 关掉旧连接，下一轮重新 wait_for_hbase / create_consumer
            try:
                if consumer is not None:
                    consumer.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            consumer = None
            conn = None
            time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    main()
