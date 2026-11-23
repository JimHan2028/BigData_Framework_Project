import json, random, time, uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
import os
MAX_ORDERS = int(os.environ.get("MAX_ORDERS", "60000"))

BOOTSTRAP = "kafka:9092"          # 容器内地址；在 Windows 本机跑则改成 localhost:29092
TOPIC = "orders"

CATEGORIES = {
    "electronics": (500, 6000),
    "clothing":    (60, 800),
    "books":       (20, 150),
    "home":        (80, 1200),
    "sports":      (90, 1500),
    "beauty":      (40, 600),
}

# 状态权重：75% 已支付、15% 已创建、10% 已取消
STATUS_POOL = ["paid"] * 75 + ["created"] * 15 + ["cancelled"] * 10

CST = timezone(timedelta(hours=8))

def make_order():
    cat = random.choice(list(CATEGORIES))
    lo, hi = CATEGORIES[cat]
    price = round(random.uniform(lo, hi), 2)
    qty = random.randint(1, 5)
    return {
        "order_id":   str(uuid.uuid4()),
        "user_id":    f"U{random.randint(1, 200):04d}",
        "product_id": f"P{random.randint(1, 120):04d}",
        "category":   cat,
        "price":      price,
        "quantity":   qty,
        "amount":     round(price * qty, 2),
        "order_time": datetime.now(CST).isoformat(timespec="seconds"),
        "status":     random.choice(STATUS_POOL),
    }

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
        linger_ms=50,
    )
    print(f"producing to {BOOTSTRAP} / topic={TOPIC}")
    n = 0
    while True:
        order = make_order()
        producer.send(TOPIC, order)
        n += 1
        if n >= MAX_ORDERS:
            producer.flush()
            print(f"[{n}] 已达上限 MAX_ORDERS={MAX_ORDERS}，停止造数，容器保活等待验收", flush=True)
            while True:
                time.sleep(3600)
        if n % 50 == 0:                      # 每 50 条刷一次、打一行
            producer.flush()
            print(f"[{n}] {order['order_time']} {order['category']:12s} "
                  f"{order['amount']:8.2f} {order['status']}")
        # 泊松间隔：均值 0.3 秒一条，约 3.3 条/秒、每小时 1.2 万条
        time.sleep(random.expovariate(1 / 0.3))