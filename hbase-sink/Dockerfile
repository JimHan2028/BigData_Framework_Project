# HBase 写入服务的镜像
# 只做一件事：装好 Kafka 客户端与 HBase 的 Thrift 客户端，然后把 consumer.py 跑起来。
# 版本固定，避免 pip 默认拉最新版导致行为漂移（happybase 1.2.0 走 Thrift，对应 hbase:9090）。
FROM python:3.11-slim

WORKDIR /app

# kafka-python 2.0.2：与 kafka 容器（Confluent 7.5.0）协议兼容
# happybase 1.2.0：HBase Thrift 客户端，替代笨重的原生 Java 客户端
RUN pip install --no-cache-dir kafka-python==2.0.2 happybase==1.2.0 lz4==4.3.2

COPY consumer.py /app/consumer.py

# -u 关闭输出缓冲，docker compose logs 里能实时看到进度
CMD ["python", "-u", "consumer.py"]
