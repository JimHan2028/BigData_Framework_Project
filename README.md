---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 74cf6cab6396794b7585ca058bcc459f_9e0b05e3e3e211f1ab13525400248c00
    ReservedCode1: e/FH8Wreg15WV/Ji5jlrbA5qKklmZpSeP7Zc++hZbpOQXBsz6UzIhGqr1LFazekqti6glvTbckHN+hMoMg+JGSSnQHWQEzUqpuDTm4wTeY0j9XFS8MhpBXUtdVijbkZaBLhtzCeTGmYiDXtNvJU4MSIbjOw4aQUGTxrbUG+i08QOUcYm6PT3anGJr10=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 74cf6cab6396794b7585ca058bcc459f_9e0b05e3e3e211f1ab13525400248c00
    ReservedCode2: e/FH8Wreg15WV/Ji5jlrbA5qKklmZpSeP7Zc++hZbpOQXBsz6UzIhGqr1LFazekqti6glvTbckHN+hMoMg+JGSSnQHWQEzUqpuDTm4wTeY0j9XFS8MhpBXUtdVijbkZaBLhtzCeTGmYiDXtNvJU4MSIbjOw4aQUGTxrbUG+i08QOUcYm6PT3anGJr10=
---

# 电商订单实时分析全栈（Docker Compose 一键跑通）

> 一句话概括：**用一条 `docker compose` 命令，在自己电脑上搭出一套完整的"电商订单数据处理流水线"**——订单从产生、传输、存储、查询、可视化，到实时流计算与离线批分析，全部跑在一个可重复启动的本地环境里。
>
> 本 README 面向**完全没有大数据基础**的读者：每一节都会先用大白话解释"这是干什么的"，再给出可以直接复制执行的命令和应该看到的结果。

---

## 目录

- [1. 这个项目到底是什么](#1-这个项目到底是什么)
- [2. 它解决什么业务问题](#2-它解决什么业务问题)
- [3. 用到的组件与各自作用](#3-用到的组件与各自作用)
- [4. 整体数据流转架构](#4-整体数据流转架构)
- [5. 完整目录结构说明](#5-完整目录结构说明)
- [6. 前置环境要求](#6-前置环境要求)
- [7. 从零开始的完整启动步骤](#7-从零开始的完整启动步骤)
- [8. 如何验证每一环节数据是否真的在流动](#8-如何验证每一环节数据是否真的在流动)
- [9. 项目最终成果在哪里看](#9-项目最终成果在哪里看)
- [10. 常见问题与排查方法](#10-常见问题与排查方法)
- [11. 项目亮点（相对"教学 demo"做了哪些生产化改造）](#11-项目亮点相对教学-demo做了哪些生产化改造)
- [12. 速查表：端口 / 常用命令 / 停止与清理](#12-速查表端口--常用命令--停止与清理)

---

## 1. 这个项目到底是什么

先建立三个最基本的概念，后面就不会晕：

1. **Docker（容器）**：可以把容器理解成一个"装好了软件和配置的冰箱"。你不需要在 Windows 上分别安装 Java、Hadoop、Kafka、Elasticsearch……每个软件都被打包成一个镜像，运行起来就是一个容器，互相隔离、互不污染，删掉容器就等于把软件彻底卸干净。
2. **docker compose（编排）**：一套系统要用十几个容器，一个个敲命令太麻烦。`docker-compose.yml` 就是一个"清单文件"，写清楚要启动哪十几个容器、谁先启动、端口怎么映射、内存给多少。一条 `docker compose up -d` 就按清单全部拉起来（`-d` 表示后台运行）。
3. **大数据组件**：处理海量数据时，单台机器的一个程序扛不住，于是把"传输、存储、计算、查询、展示"拆成专门的角色各干各的，用网络连成一条流水线。本项目把这些角色全部用容器的方式装进了你的电脑。

本项目**不是**一个"只有几行代码的玩具 demo"，而是一条**端到端、可验收、能重复启动**的数据管道：从造订单数据开始，到 Kibana 出图、Hive 出 SQL 查询结果、Flink 出实时聚合流、Spark 出离线指标表，每一段都有对应的验证命令。

三条基础链路 + 两条扩展链路（下文简称 A/B/C/D/E）：

| 链路 | 走向 | 定位 |
|---|---|---|
| **A** | Kafka → Logstash → Elasticsearch → Kibana | 实时看板（秒级看到订单在涨） |
| **B** | Kafka → Logstash → HDFS → Hive | 离线明细查询（用 SQL 查订单原始表） |
| **C** | Kafka → HBase Sink → HBase → Hive 映射表 | NoSQL 宽表查询（按订单号直接取整行） |
| **D**（扩展） | Kafka → Flink（1 分钟事件时间窗口）→ 控制台 + Kafka `orders_flink_agg` | 实时流计算（窗口内 GMV、支付/取消笔数） |
| **E**（扩展） | HDFS → Spark SQL → Parquet（4 组指标） | 离线批分析（品类、每日、时段、Top10 用户） |

A/B/C 是基础链路（`--profile ext` 一起启动，其中还有 Kafka-UI 与 Redis 两个辅助容器）；D/E 挂在独立 profile（`flink` / `spark`）下，按需启动，因为它们很吃内存。

---

## 2. 它解决什么业务问题

假设你在经营一个电商网站，每天产生几十万到上亿条订单。老板和运营会不断问这些问题：

| 业务问题 | 靠哪条链路回答 | 具体形式 |
|---|---|---|
| 现在卖得怎么样？各品类订单量、销售额谁高谁低？ | 链路 A | Kibana 看板「电商订单实时分析」：饼图（品类订单占比）、柱图（GMV）、折线（订单量趋势） |
| 这一天到底下了多少单？能不能用 SQL 随意筛？ | 链路 B | Hive 表 `ods.orders_raw`，用 SQL 做任意条件统计 |
| 给我某个订单号，把这一单的全部字段取出来 | 链路 C | HBase 表 `orders`（订单号即行键，毫秒级点查），Hive 映射表 `ods.orders_hbase` 也能查 |
| 每分钟各品类的成交额、支付笔数、取消笔数是多少？ | 链路 D | Flink 按"品类 + 1 分钟事件时间滚动窗口"聚合，结果打到日志并写回 Kafka topic `orders_flink_agg` |
| 做一份阶段性复盘：品类销售概况、每日 GMV、下单时段分布、消费 Top10 用户 | 链路 E | Spark 批处理，结果写成 4 份 Parquet，落在 HDFS `/data/spark/` 下 |

一句话：**同一条订单数据流，被三条通路分别"实时看板化""明细 SQL 化""宽表点查化"，再用两种计算引擎分别验证"实时流处理"和"离线批处理"两种能力。**

---

## 3. 用到的组件与各自作用

### 3.1 组件清单（大白话版）

| 组件 | 版本（见 `.env` / `docker-compose.yml`） | 它是什么 | 在本项目里干什么 |
|---|---|---|---|
| **ZooKeeper** | confluentinc/cp-zookeeper `7.5.0` | 分布式系统的"协调员/通讯录" | Kafka 用它保存集群元数据（谁是 broker、topic 有哪些） |
| **Kafka** | confluentinc/cp-kafka `7.5.0` | 高吞吐"消息队列"，可以想成一条不断向前走的传送带 | 所有链路的**统一数据源**：topic `orders`（3 分区、lz4 压缩、保留 72 小时）；扩展 D 的聚合结果写回 topic `orders_flink_agg` |
| **Elasticsearch** | `7.17.28` 单节点 | 专门做"搜索与聚合"的数据库 | 链路 A 的存储，索引名 `orders-YYYY.MM.dd`（按天滚动），文档主键用 `order_id`（天然幂等去重） |
| **Kibana** | `7.17.28`（界面中文 `zh-CN`） | Elasticsearch 的图形界面 | 链路 A 的可视化看板（索引模式 `orders*`，时间为 `order_time`） |
| **Logstash**（自建镜像） | `7.17.28` + `logstash-output-webhdfs` 插件 | "搬运工 + 简单加工" | 从 Kafka 消费订单，做字段清洗、时间校正，然后**一份写 ES、一份写 HDFS**（通过 `pipelines.yml` 拆成两条独立管线） |
| **HDFS**（NameNode + DataNode） | bde2020 Hadoop `3.2.1` | 分布式文件系统，可理解为"很多台机器拼成的大硬盘" | 链路 B 的落地存储：`/data/orders/dt=YYYY-MM-DD/orders-HH.csv`（按天分区、按小时分文件） |
| **Hive**（HiveServer2 + PostgreSQL 元数据库） | bde2020 hive `2.3.1-postgresql-metastore` | 让你能**用 SQL 查文件**的引擎（把文件当表） | 链路 B：外部表 `ods.orders_raw` 指向 `/data/orders`；链路 C：映射表 `ods.orders_hbase` 直接查 HBase |
| **HBase** | bde2020 `hbase-standalone 1.0.0-hbase1.2.6` | 分布式 NoSQL 宽表（按行键极速点查） | 链路 C 的存储：表 `orders`、列族 `info`、行键 = `order_id`，并额外拉起 **Thrift 9090** 端口供 Python 客户端写入 |
| **orders-gen**（自建） | Python 3.11 + kafka-python 2.0.2 | 订单"造数器" | 按泊松间隔（均值 0.3 秒/条，约 3.3 条/秒）持续生成订单 JSON 写入 Kafka，上限 `MAX_ORDERS=60000` 条 |
| **hbase-sink**（自建） | Python 3.11 + happybase 1.2.0 | Kafka 到 HBase 的"写库小工" | 每 100 条做一次批量写入，**先落库成功再提交消费位点**，避免丢数据 |
| **hive-init**（一次性） | 与 Hive 同镜像 | 初始化任务 | 等 HiveServer2 就绪 → 建 `ods.orders_raw`、`ods.orders_hbase` 两张表 → 等 HDFS 出现 `dt=` 分区 → `MSCK REPAIR TABLE` 注册分区 → 校验行数 → 退出（`Exit 0` 正常） |
| **Kafka-UI**（profile `ext`） | provectuslabs/kafka-ui `v0.6.2` | Kafka 图形界面 | 浏览器里看 topic、分区、消息堆积（http://localhost:8080） |
| **Redis**（profile `ext`） | `redis:7-alpine`（200MB 上限 + LRU 淘汰） | 内存键值缓存 | 预留的缓存/去重扩展位，基础链路不依赖它 |
| **Flink**（profile `flink`） | `flink:1.17.2-scala_2.12-java11`（JobManager + TaskManager + 一次性 submit） | 实时流计算引擎 | 链路 D：读 Kafka `orders`，按品类 1 分钟事件时间滚动窗口聚合，结果 print 到日志 + 写回 Kafka |
| **Spark**（profile `spark`） | `apache/spark:3.5.1`（Master + 一次性 batch） | 离线批处理引擎 | 链路 E：读 HDFS `/data/orders`，Spark SQL 算出 4 组指标写 Parquet 到 `/data/spark/` |

### 3.2 数据长什么样（贯穿全项目的唯一结构）

造数脚本 `orders-gen/producer.py` 生成的每一条订单是一个 JSON，字段固定如下（也是后续所有组件的契约）：

```json
{
  "order_id": "8f1b0c9e-...（UUID）",
  "user_id": "U0123",
  "product_id": "P0045",
  "category": "electronics",
  "price": 3288.50,
  "quantity": 2,
  "amount": 6577.00,
  "order_time": "2026-11-23T10:21:05+08:00",
  "status": "paid"
}
```

- `category` 取值固定 6 种：`electronics`、`clothing`、`books`、`home`、`sports`、`beauty`，每种有各自的单价区间（例如 `books` 20–150 元、`electronics` 500–6000 元）。
- `status` 按权重随机：**75% `paid`（已支付）、15% `created`（已创建）、10% `cancelled`（已取消）**。
- `amount = price × quantity`，保留两位小数。
- `order_time` 使用**东八区**时间并带 `+08:00` 时区偏移（后面所有时间处理都依赖这个字段）。

---

## 4. 整体数据流转架构

### 4.1 全景图

```
                     ┌─────────────────────────┐
                     │  orders-gen（造数容器）  │  每 0.3 秒左右 1 条 JSON
                     └───────────┬─────────────┘
                                 │ 生产
                                 ▼
                     ┌─────────────────────────┐
                     │  Kafka  topic: orders   │  3 分区 / lz4 / 保留 72h
                     └───────────┬─────────────┘
             ┌───────────────────┼────────────────────┐
             │ 消费组 logstash-es │ 消费组 logstash-hdfs│ 消费组 hbase-sink
             ▼                   ▼                    ▼
   ┌──────────────────┐  ┌───────────────┐   ┌──────────────────┐
   │ Logstash 管线 es │  │Logstash 管线  │   │  hbase-sink      │
   │ 清洗/时间校正    │  │hdfs（WebHDFS）│   │ 批量 100 条写入   │
   └────────┬─────────┘  └───────┬───────┘   └────────┬─────────┘
            │                    │                     │
            ▼                    ▼                     ▼
  ┌──────────────────┐  ┌───────────────┐   ┌──────────────────┐
  │ Elasticsearch    │  │     HDFS      │   │      HBase       │
  │ orders-YYYY.MM.dd│  │/data/orders/  │   │ 表 orders(列族   │
  │ 显式 strict 模板 │  │ dt=YYYY-MM-DD │   │ info, 行键=订单号)│
  └────────┬─────────┘  │ /orders-HH.csv│   └────────┬─────────┘
           │            └───┬───────┬───┘            │
           ▼                │       │                │ HBaseStorageHandler
   ┌──────────────┐         │       │                ▼
   │   Kibana     │         ▼       ▼       ┌──────────────────────┐
   │ 看板/Discover│   ┌──────────┐ ┌─────────┐│ Hive 映射表          │
   └──────────────┘   │  Hive    │ │  Spark  ││ ods.orders_hbase     │
        （链路 A）    │ 外部表   │ │ 批分析  │└──────────────────────┘
                      │ods.      │ │（链路 E）│        （链路 C）
                      │orders_raw│ │Parquet  │
                      └──────────┘ └─────────┘
                        （链路 B）    ↓
                                /data/spark/{category_stats,
                                daily_stats,hourly_stats,top_users}

   另：Kafka orders ──► Flink（1 分钟事件时间滚动窗口，按品类）──► 控制台 + Kafka orders_flink_agg
                                                                     （链路 D）
```

### 4.2 一句话说清三条基础链路的"为什么这样设计"

- **链路 A 为什么快**：Logstash 用 `pipeline.batch.size`（默认 125）攒小批写 ES，ES 索引 `refresh_interval` 设为 `5s`，所以打开 Kibana 后几秒内就能看到数字在涨；同时索引按天滚动（`orders-%{+YYYY.MM.dd}`），避免单索引无限膨胀。
- **链路 B 为什么要落文件**：数据仓库的底座仍是"便宜的大硬盘"。Logstash 通过 WebHDFS 写 `/data/orders/dt=YYYY-MM-DD/orders-HH.csv`，Hive 外部表只做"挂载"（`LOCATION '/data/orders'`），所以文件怎么来、Hive 都能查；`dt` 目录名自动成为分区列。
- **链路 C 为什么还要一份 HBase**：ES 擅长搜索与聚合但存储成本高，HDFS/Hive 擅长批量扫描但点查慢。HBase 用订单号做行键，适合"按 id 取整行"；再用 Hive 的 `HBaseStorageHandler` 建映射表，就能用 SQL 访问同一份数据。

### 4.3 时间口径的关键处理（容易被忽略但很重要）

Logstash 两条管线里都有这一段：

```ruby
date {
  match  => ["order_time", "ISO8601"]
  target => "@timestamp"
}
```

它的作用是**用业务时间（下单时间）覆盖 `@timestamp`（入库时间）**。不这么做，Kibana 时间轴上显示的是"什么时候被搬运进库"，补历史数据时会和真实下单时间差好几天；索引名 `orders-%{+YYYY.MM.dd}` 也会跟着偏移（北京时间 0–8 点的订单会落到"前一天"的索引里）。容器统一设了 `TZ=Asia/Shanghai`，保证格式化后日期正确。

---

## 5. 完整目录结构说明

```
bigdata-project/
├── docker-compose.yml            # 核心：20 个服务定义 + 3 个命名卷 + 各服务内存上限/健康检查/日志轮转
├── .env                          # 版本锁定（CP_VERSION / ES_VERSION / HADOOP_VERSION）、各 JVM 堆上限、Kafka 生产参数、TZ
├── .gitignore                    # 忽略 .env、构建产物、Python 缓存、日志、IDE 配置等
├── start.ps1 / start.sh          # 一键启动（Docker 检查 → 端口检查 → 按需拉镜像 → 构建自建镜像 → up → 等建表 → 导入 Kibana 看板）
├── stop.ps1  / stop.sh           # 一键停止（down，不加 -v，数据卷保留）
├── verify.ps1 / verify.sh        # 一键验收（逐条链路打 [通过]/[待查]；-Ext 会把 Flink/Spark 真跑一遍再判）
│
├── orders-gen/                   # 【造数】Kafka 生产者
│   ├── Dockerfile                # python:3.11-slim + kafka-python==2.0.2
│   └── producer.py               # 6 品类 + 3 状态加权随机；泊松间隔；MAX_ORDERS=60000 到量停造但容器保活
│
├── hbase-sink/                   # 【链路 C 第一段】Kafka → HBase
│   ├── Dockerfile                # + happybase==1.2.0（走 Thrift 9090）+ lz4
│   └── consumer.py               # 幂等建表、批量 put(100)、落库成功才 commit 位点、异常自动重连
│
├── logstash/
│   ├── build/Dockerfile          # 官方 7.17.28 + 安装 logstash-output-webhdfs 插件
│   ├── config/pipelines.yml      # 把 es / hdfs 拆成两条独立 pipeline（避免重复消费与字段互相污染）
│   ├── pipeline/es.conf          # Kafka → 清洗/时间校正 → Elasticsearch（显式索引模板、document_id=order_id）
│   ├── pipeline/hdfs.conf        # Kafka → 清洗 → grok 提取 dt/hour → WebHDFS 写 CSV（flush 10 条 / 空闲 5 秒）
│   └── template/orders-template.json  # ES 索引模板：dynamic=strict、6 个字段类型固定、1 分片 0 副本、refresh 5s
│
├── hive/
│   ├── conf/hbase-site.xml       # HBase 客户端配置（quorum=hbase:2181、znode /hbase、RPC 超时 60s），挂进 hive-server 与 hive-init
│   └── init/
│       ├── init.sh               # 等 HiveServer2 → 执行两段 SQL → 等 HDFS 分区 → MSCK REPAIR → 校验行数 → 退出
│       ├── 01_orders_raw.sql     # 建库 ods + 外部表 orders_raw（逗号分隔 TEXTFILE，按 dt 分区，指向 /data/orders）
│       └── 02_orders_hbase.sql   # 建 HBase 映射表 orders_hbase（列映射 ":key,info:user_id,..."）
│
├── hbase/init/
│   ├── start-with-thrift.sh      # 先 start-hbase.sh，等 20 秒，再拉起 thrift 9090，最后 tail 日志保活
│   └── create_table.sh           # 手工建表备选脚本（create 'orders','info'，可反复执行）
│
├── flink/                        # 【链路 D】实时流计算
│   ├── src/com/demo/OrderRealtimeJob.java   # Kafka Source → 解析 → 水印(乱序容忍 5s/空闲 30s) → keyBy(category) → 1 分钟滚动窗口 → print + Kafka Sink
│   ├── build.sh                  # 借 eclipse-temurin:11-jdk 镜像 + flink 镜像自带 lib 编译，产出 flink-orders-job.jar
│   ├── flink-orders-job.jar      # 预编译作业包（flink-submit 直接提交，不改代码无需重编）
│   ├── lib/                      # flink-connector-kafka-1.17.2 / flink-connector-base-1.17.2 / kafka-clients-3.2.3（挂进 /opt/flink/lib）
│   └── build/classes/            # 编译中间产物（已在 .gitignore 中忽略）
│
├── spark/                        # 【链路 E】离线批分析
│   └── orders_batch_analysis.py  # 先用 WebHDFS 轮询等数据（默认最多 10 分钟）→ 读 CSV 并自动识别 dt 分区 → 4 组指标 → 写 Parquet
│
├── kibana/
│   ├── saved_objects.ndjson      # 索引模式 orders*（时间字段 order_time）+ 3 个可视化 + Dashboard「电商订单实时分析」，start 脚本自动导入
│   └── export.ndjson             # 看板导出备份
│
└── docs/                         # 运行截图
    ├── 系统架构图.png
    ├── kibana-dashboard.png
    └── kibana-discover.png
```

几个容易误解的地方先说明：

- **`.env` 是隐藏文件**（以点开头）。复制、打包、解压时如果漏掉它，`${CP_VERSION}`、`${ES_VERSION}` 这些变量会变成空值，`docker compose up` 会直接报错。
- `start.ps1` / `verify.ps1` 是 **Windows PowerShell 专用**；`start.sh` / `verify.sh` 是 macOS / Linux 版，二者功能等价。
- `flink/build/` 与 `flink-orders-job.jar` 的关系：`build/` 是编译过程的中间产物（`.class`），`.jar` 才是 `flink-submit` 真正提交的东西。**不改 Java 源码就不需要重新编译**。

---

## 6. 前置环境要求

### 6.1 软件

| 项 | 要求 | 怎么确认 |
|---|---|---|
| **Docker Desktop** | 4.x 及以上（必须自带 **Compose V2**，即 `docker compose` 命令，注意没有中间的横杠） | `docker compose version` 能打印版本号 |
| **WSL2**（Windows） | Docker Desktop 使用 WSL2 后端 | Docker Desktop → Settings → General 勾选 *Use the WSL 2 based engine* |
| **PowerShell** | Windows PowerShell 5.1 或 PowerShell 7 | `.\start.ps1` 能执行（若提示禁止运行脚本，用 `powershell -ExecutionPolicy Bypass -File .\start.ps1`） |
| **浏览器** | Chrome / Edge 等现代浏览器 | 用于打开 Kibana、Kafka-UI、HDFS/HBase 控制台 |

> 这个仓库**不需要**你在本机安装 Java、Python、Hadoop、Kafka、Spark —— 全部在容器里。本机只要 Docker。

### 6.2 硬件资源（最容易被忽略，也是失败率最高的原因）

| 项 | 建议值 | 说明 |
|---|---|---|
| **内存** | 基础链路 **10 GB 可用给 Docker**；叠加 Flink + Spark 建议 **12 GB 以上** | `docker-compose.yml` 里各容器都设了 `deploy.resources.limits.memory`：ES 1400M、HBase 1600M、Kafka 1024M、Kibana 900M、Logstash 900M、HiveServer2 900M、Flink TaskManager 1200M 等，合计约 10–14 GB |
| **磁盘** | 首次拉取镜像约 **13 GB**，另需约 **5 GB** 运行数据 | 自有镜像 3 个（orders-gen / hbase-sink / logstash）+ Hadoop/Hive/HBase/Flink/Spark 等公共镜像 |
| **CPU** | 4 核以上 | 20 个容器同时跑，2 核会明显卡 |

设置方法：Docker Desktop → **Settings → Resources → Memory**，把内存调到 10–12 GB（默认常见为 8 GB，**不够**，会导致 Elasticsearch、Kafka 反复重启）。

### 6.3 需要空闲的端口

| 端口 | 用途 | 端口 | 用途 |
|---|---|---|---|
| 2181 | ZooKeeper | 9000 | HDFS RPC |
| 9092 / 29092 | Kafka（容器内 / 宿主机） | 9870 | HDFS NameNode Web UI |
| 9200 | Elasticsearch | 10000 / 10002 | HiveServer2 JDBC / Web UI |
| 5601 | Kibana | 16010 / 9090 / 16020 | HBase Master UI / Thrift / RegionServer |
| 9600 | Logstash 监控 API | 8080 | Kafka-UI（ext） |
| 6379 | Redis（ext） | 8081 | Flink Web UI（flink） |
| 8082 / 7077 | Spark Master UI / RPC（spark） | 4040 | Spark 应用 UI（仅作业运行期间，spark） |

启动脚本会**自动检查**这些端口；如果被别的程序占用会提示，如果是本项目容器自己在用则放行（支持重复执行 `start.ps1`）。

### 6.4 网络

- 首次启动需要从 Docker Hub 与 `docker.elastic.co` 拉镜像，国内网络常需镜像加速器（见第 10 节）。
- `start.ps1` / `start.sh` 采用**按需拉取**：先比对本地镜像，齐全就完全跳过 `pull`；只拉缺的那几个。

---

## 7. 从零开始的完整启动步骤

### 第 0 步：确认 Docker 在跑

```powershell
docker info                 # 能打印一堆信息就说明正常
docker compose version      # 应显示 v2.x
```

如果 `docker info` 报错，先打开 Docker Desktop，等左下角状态变成 **running**。

### 第 1 步：进入项目目录

```powershell
cd C:\Users\Administrator\Desktop\bigdata-project
```

> 注意：必须在这个目录下执行（`docker-compose.yml` 与 `.env` 都在这一层）。路径里有中文一般没问题，但**不要**放在 OneDrive 同步目录里，同步会拖慢甚至锁住文件。

### 第 2 步：一键启动（推荐）

```powershell
powershell -ExecutionPolicy Bypass -File .\start.ps1
```

macOS / Linux：

```bash
chmod +x start.sh verify.sh stop.sh
./start.sh
```

脚本按顺序做这些事（命名与提示语和源码一致）：

1. `[步骤] 检查 Docker 是否已启动` → `[ OK ] Docker 与 Compose 就绪`
2. 读取本机物理内存，低于 12 GB 会 `[警告]`（只影响是否要同时开 Flink/Spark）
3. 检查端口占用，区分"本项目容器"与"外部进程"
4. 按需拉取镜像；`bde2020/hive:2.3.1-postgresql-metastore` 若已被官方下架，脚本会自动 `pull latest` 再 `tag` 成原版本号（两者 digest 相同，内容一致）
5. 构建 3 个自建镜像（orders-gen / hbase-sink / logstash）
6. `docker compose --profile ext ... up -d` 启动基础链路
7. 等待 `hive-init` 退出，看到 `[ OK ] hive-init 已完成建表（Exited 0，正常现象）`
8. 等 Kibana 就绪后**自动导入** `kibana/saved_objects.ndjson`（索引模式 `orders*` + Dashboard「电商订单实时分析」）
9. 打印访问入口：

```
================ 访问入口 ================
  Kibana           http://localhost:5601
  Kafka UI         http://localhost:8080
  HDFS NameNode    http://localhost:9870
  HBase Master     http://localhost:16010
  Elasticsearch    http://localhost:9200
  HiveServer2      jdbc:hive2://localhost:10000
==========================================
```

**首次启动需要 10–30 分钟**（拉镜像 + 构建），之后再次启动通常 2–3 分钟。

### 第 3 步（可选）：把 Flink / Spark 也拉起来

```powershell
.\start.ps1 -Stage flink      # 只加 Flink
.\start.ps1 -Stage spark      # 只加 Spark
.\start.ps1 -Stage all        # 全起（内存需 12GB 以上）
```

或者用验收脚本一条命令搞定"真跑一遍 + 立即验收"（推荐）：

```powershell
.\verify.ps1 -Ext
```

### 第 4 步：等 2–4 分钟再开始查数据

`orders-gen` 是持续造数的，Logstash 需要攒批（batch 默认 125 条 / 50ms）才写 ES 与 HDFS，Hive 还要等分区出现才注册。**刚启动就查不到数据属于正常现象**，耐心等 2–4 分钟。

### 3.4 手动启动（不想用脚本时）

```powershell
# 基础链路（必须带 --profile ext，否则 kafka-ui 与 redis 不会启动）
docker compose --profile ext up -d --build

# 扩展：Flink
docker compose --profile flink up -d flink-jobmanager flink-taskmanager
docker compose --profile flink up flink-submit        # 一次性容器，提交完即退出

# 扩展：Spark
docker compose --profile spark up -d spark
docker compose --profile spark up spark-batch         # 一次性容器，跑完即退出

# 一次全起
docker compose --profile ext --profile flink --profile spark up -d
```

### 7.5 预期的容器状态

```powershell
docker compose --profile ext ps
```

- 常驻容器应为 `Up (healthy)` 或 `Up`：zookeeper、kafka、elasticsearch、kibana、logstash、namenode、datanode、hive-metastore-postgresql、hive-server、hbase、orders-gen、hbase-sink、kafka-ui、redis。
- `hive-init` 为 `Exited (0)` —— **正常**，它是一次性初始化任务。
- Flink/Spark 的 `flink-submit`、`spark-batch` 为 `Exited (0)` —— 同样**正常**。

---

## 8. 如何验证每一环节数据是否真的在流动

原则：**从上游往下游逐段验证**，某一段断了就知道问题出在哪。所有命令都在项目根目录执行。

### 8.0 一键总验收（先跑这个）

```powershell
.\verify.ps1            # 只检查，不启动任何东西
.\verify.ps1 -Ext       # 起 Spark + Flink 真跑一遍，然后立即验收
```

它会逐条打印 `[通过] / [待查]`，检查项与脚本内一致：

| 检查项 | 通过标准 |
|---|---|
| 容器运行数 | 运行中容器数达到阈值（基础链路 14 个，起 Flink 后 16 个） |
| 链路A ES 文档数 | `orders-*` 文档数 > 0 |
| 链路B HDFS 分区 | `/data/orders` 下出现 `dt=` 分区目录 |
| 链路B Hive orders_raw | `select count(1)` 能返回行数 |
| 链路C HBase orders 表 | `count 'orders'` 行数 > 0 |
| 链路C Hive 映射表 | 按 `category` 分组查询有返回 |
| 扩展D Flink 作业 | 8081 上存在 `RUNNING` 作业 |
| 扩展E Spark 输出 | 4 个 Parquet 目录齐全，且 `spark-batch` 在 30 分钟内以 `ExitCode=0` 完成（**旧运行残留不算通过**） |

### 8.1 第 0 环：订单是否在产生（Kafka 源头）

```powershell
docker logs orders-gen --tail 20
```

应看到类似（每 50 条打一行）：

```
[50] 2026-11-23T10:21:05+08:00 electronics   6577.00 paid
[100] 2026-11-23T10:21:21+08:00 books           88.50 created
```

数字一直往上走，说明造数正常。到 60000 条后会打印 `已达上限 MAX_ORDERS=60000，停止造数，容器保活等待验收`，然后不再输出——**这是设计好的行为**，不是卡死。

再确认 Kafka 里真有消息：

- 浏览器打开 http://localhost:8080（Kafka-UI）→ Topics → `orders`，能看到 3 个分区与消息总量；
- 或命令行：

```powershell
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --max-messages 3 --timeout-ms 10000
```

应打印 3 条订单 JSON。

### 8.2 链路 A：Kafka → Logstash → Elasticsearch → Kibana

```powershell
# 1) Logstash 两条 pipeline 都应处于 running
curl -s http://localhost:9600/_node/pipelines?pretty

# 2) ES 里的文档数（N 会随时间增长）
curl -s "http://localhost:9200/orders-*/_count"

# 3) 看一眼实际文档结构
curl -s "http://localhost:9200/orders-*/_search?size=1&pretty"
```

判定标准：第 2 条返回 `{"count":N,...}` 且 **N 大于 0 并在增长**；第 3 条能看到 `order_id / user_id / category / amount / status / order_time / @timestamp` 等字段，且 `@timestamp` 与 `order_time` 指向同一时刻（证明时间校正生效）。

### 8.3 链路 B：Kafka → Logstash → HDFS → Hive

```powershell
# 1) HDFS 是否出现按天分区、按小时的文件
docker exec namenode hdfs dfs -ls /data/orders
docker exec namenode hdfs dfs -ls /data/orders/dt=$(Get-Date -Format yyyy-MM-dd)

# 2) 随便看一行文件内容（逗号分隔、无表头）
docker exec namenode hdfs dfs -cat "/data/orders/dt=$(Get-Date -Format yyyy-MM-dd)/orders-$(Get-Date -Format HH).csv" 2>$null | Select-Object -First 2
```

应看到 `dt=2026-11-23` 之类的目录，里面是 `orders-08.csv`、`orders-09.csv`……（每小时一个文件）。文件内容是 9 个字段的逗号分隔行：

```
order_id,user_id,product_id,category,price,quantity,amount,order_time,status
```

再用 Hive SQL 验证：

```powershell
# 行数
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "select count(1) from ods.orders_raw;"

# 各品类 GMV（只看已支付）
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --outputformat=tsv2 -e "select category, count(1) cnt, round(sum(case when status='paid' then amount else 0 end),2) gmv from ods.orders_raw group by category order by gmv desc;"
```

判定标准：行数 > 0 并持续增长；第二条能返回 6 个品类的计数与 GMV。

### 8.4 链路 C：Kafka → hbase-sink → HBase → Hive 映射表

```powershell
# 1) 写入进程日志（每满 100 条打一行累计数）
docker logs hbase-sink --tail 10

# 2) HBase 表行数
docker exec hbase bash -c "echo \"count 'orders'\" | hbase shell -n"

# 3) 取 1 行看列族 info 的字段
docker exec hbase bash -c "echo \"scan 'orders', {LIMIT=>1}\" | hbase shell -n"

# 4) Hive 映射表按品类分组
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --outputformat=tsv2 -e "select category,count(1) c from ods.orders_hbase group by category;"
```

判定标准：`hbase-sink` 日志出现 `已写入 HBase 累计 N 条`；`count 'orders'` 返回行数 > 0；Hive 分组查询能返回 6 个品类。

> **重要坑（务必注意）**：验证链路 C **不要**用 `select count(1) from ods.orders_hbase`。Hive 查询 HBase 映射表时若没有任何谓词，会走元数据短路直接返回 0，而数据其实是存在的。要看数据请用 `group by` 汇总，或 `select * from ods.orders_hbase limit 5` 直接看行。`ods.orders_raw`（普通文件表）用 `count(1)` 则完全正常。

### 8.5 扩展 D：Flink 实时窗口聚合

```powershell
# 1) 作业是否在跑（应看到 state=RUNNING）
curl -s http://localhost:8081/jobs/overview

# 2) 控制台聚合输出（按 品类 + 1 分钟窗口）
docker logs flink-taskmanager 2>&1 | Select-String "FLINK_AGG" | Select-Object -Last 10

# 3) 写回 Kafka 的聚合流
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders_flink_agg --from-beginning --max-messages 5 --timeout-ms 15000
```

单条输出形如：

```json
{"window_start":"2026-11-23 10:21:00","window_end":"2026-11-23 10:22:00","category":"electronics","order_cnt":32,"paid_cnt":27,"cancel_cnt":3,"gmv":321471.24,"avg_amount":12129.04}
```

指标口径（摘自 `OrderRealtimeJob.CategoryWindowAgg`）：按 `category` + **1 分钟事件时间滚动窗口**聚合 → `order_cnt` 下单笔数、`paid_cnt` 支付笔数、`cancel_cnt` 取消笔数、`gmv` 已支付金额合计、`avg_amount` 客单价（该窗口内全部订单的平均金额）。作业用 `order_time` 做事件时间，水印容忍 **5 秒**乱序、空闲分区 **30 秒**，从最早位点开始消费（所以作业重启会把历史订单重放一遍）。

### 8.6 扩展 E：Spark 离线批分析

```powershell
# 1) 跑批日志（脚本先等 HDFS 有数据，默认最多 10 分钟）
docker logs spark-batch 2>&1 | Select-String -Pattern "Spark|----" | Select-Object -Last 40

# 2) 4 个 Parquet 结果目录
docker exec namenode hdfs dfs -ls /data/spark
```

日志里会依次打印 4 组指标表；HDFS 上应出现：

| 指标 | 输出路径 | 内容 |
|---|---|---|
| 品类销售概况 | `/data/spark/category_stats` | 订单数、GMV（仅 paid）、客单价、取消数、取消率(%) |
| 每日订单与 GMV | `/data/spark/daily_stats` | 按 `dt` 分区：订单数、GMV、UV（去重用户数） |
| 下单时段分布 | `/data/spark/hourly_stats` | 0–23 时各小时订单数与金额合计 |
| 消费 Top10 用户 | `/data/spark/top_users` | 仅 paid 订单，按累计金额降序 Top10 |

> **注意时效性**：`/data/spark` 下这 4 个目录是 `spark-batch` **上一次**运行的产物，不删就一直存在。即使 `spark-batch` 早已 `Exited`，目录也还在——目录存在**不代表**这次的作业跑过。想重跑：`docker compose --profile spark up -d --force-recreate spark-batch`，再 `docker logs -f spark-batch`。

### 8.7 重新编译 Flink 作业（只有改了 Java 代码才需要）

```bash
./flink/build.sh                  # 用 eclipse-temurin:11-jdk 镜像编译，产出 flink/flink-orders-job.jar
docker compose --profile flink up flink-submit
```

`flink-orders-job.jar` 已在仓库里，**不改代码无需编译**。

---

## 9. 项目最终成果在哪里看

按"最直观 → 最技术"的顺序排列：

### 9.1 Kibana 实时看板（首选，最直观）

1. 浏览器打开 http://localhost:5601 （界面已是中文）。
2. 左侧菜单 **Dashboard（仪表板）** → 打开 **「电商订单实时分析」**。
3. 看板上是 3 个图（来自 `kibana/saved_objects.ndjson`）：
   - **订单数按类别分布**（饼图，按 `category` 统计订单数）；
   - **各类别销售额GMV**（柱状图，按 `category` 求 `amount` 之和）；
   - **订单量时间趋势**（折线图，按 `order_time` 做时间直方图）。
4. 看板默认时间范围是 `now-1h`、每 30 秒自动刷新。如果图是空的，把右上角时间范围改成 **Last 4 hours / Today**，或点一下刷新。
5. 想自己随便筛：左侧 **Discover**，索引模式选 `orders*`，可按 `category`、`status` 过滤，点开单条能看到完整 JSON。

> 截图参考：`docs/kibana-dashboard.png`、`docs/kibana-discover.png`。

### 9.2 Hive SQL 查询结果（明细分析）

```powershell
# 支付订单的品类排行
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --outputformat=tsv2 -e "select category, count(1) cnt, round(sum(amount),2) amt from ods.orders_raw where status='paid' group by category order by amt desc;"

# 按小时看下单量
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --outputformat=tsv2 -e "select substr(order_time,12,2) hh, count(1) from ods.orders_raw group by substr(order_time,12,2) order by hh;"
```

Hive Web UI 在 http://localhost:10002 ，NameNode 文件浏览在 http://localhost:9870 （Utilities → Browse the file system → `/data/orders`）。

### 9.3 HBase 宽表查询

- Web UI：http://localhost:16010 （看表 `orders`、RegionServer 状态）。
- Shell：

```powershell
docker exec hbase bash -c "echo \"scan 'orders', {LIMIT=>5}\" | hbase shell -n"
```

- 或用 Hive 映射表以 SQL 方式查同一份数据（`ods.orders_hbase`）。

### 9.4 Flink 实时聚合结果

- Web UI：http://localhost:8081 （作业拓扑、每个算子的输入输出条数、水位线、背压）。
- 文本结果：`docker logs flink-taskmanager | Select-String FLINK_AGG`。
- 数据流：Kafka topic `orders_flink_agg`。

### 9.5 Spark 离线指标（Parquet）

- HDFS：`/data/spark/{category_stats,daily_stats,hourly_stats,top_users}`，可在 http://localhost:9870 浏览。
- 控制台表格：`docker logs spark-batch`。
- 应用 UI（作业运行期间）：http://localhost:4040 ；集群视角：http://localhost:8082 。想多留一会儿截图，先设 `$env:SPARK_UI_HOLD_SECONDS="120"` 再 `--force-recreate spark-batch` 重跑。

### 9.6 数据链路健康度

- Logstash 两条管线的吞吐/事件数：http://localhost:9600/_node/stats
- Kafka topic 与消费组堆积：http://localhost:8080 → Consumers / Topics

---

## 10. 常见问题与排查方法

排查通用套路：**先看容器状态，再看容器日志，最后看端口**。

```powershell
docker compose --profile ext --profile flink --profile spark ps
docker logs <容器名> --tail 100
docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' <容器名>
```

### 10.1 这些"异常现象"其实都正常

| 现象 | 解释 |
|---|---|
| `hive-init` 显示 `Exited (0)` | 一次性初始化容器，建完表、注册完分区、校验完行数就退出。日志末尾应是 `orders_raw 已有 N 行，初始化完成` |
| `orders-gen` 到 60000 条后不再打印日志 | 造数有上限（`MAX_ORDERS`），到量后 flush 并保活等验收。想造更多：改 `docker-compose.yml` 里 `orders-gen` 的 `MAX_ORDERS` 后重建该容器 |
| `flink-submit`、`spark-batch` 显示 `Exited (0)` | 两者都是一次性容器：前者提交完作业就退出（作业仍在 TaskManager 上跑），后者跑完批分析退出。看结果分别查 `docker logs flink-taskmanager`、`docker logs spark-batch` |
| http://localhost:4040 打不开 | 4040 是 Spark **应用 UI**，只有 `spark-batch` 运行期间（或 `SPARK_UI_HOLD_SECONDS` 保留窗口内）存在，作业结束端口就消失。想看集群用 http://localhost:8082 |
| 刚启动查不到数据 | Logstash 攒批 + Hive 等分区，需 2–4 分钟 |
| Kibana 图表是空的 | 多半是时间范围太窄，改成 Last 4 hours / Today |

### 10.2 镜像拉取失败 / 卡住

- 国内直连 Docker Hub 通常不可用：Docker Desktop → **Settings → Docker Engine** 配置 registry mirror。
- `docker.elastic.co`（ES / Kibana / Logstash）**不在**常规加速器覆盖范围，需要单独代理或提前在有网环境 `docker pull` 后导出。
- `start.ps1` / `start.sh` 是**按需拉取**：本地已有则完全跳过 pull。

### 10.3 报 `bde2020/hive:2.3.1-postgresql-metastore ... no such manifest`

不是网络问题：该 tag 已被作者下架，仓库只剩 `latest`，而 `latest` 与该 tag 是**同一个 digest**，内容完全一致。启动脚本已内置补位：

```powershell
docker pull bde2020/hive:latest
docker tag  bde2020/hive:latest bde2020/hive:2.3.1-postgresql-metastore
```

### 10.4 报 `network ... not found` / `failed to set up container networking`

Docker 网络被重建过（Docker Desktop 重启，或上次 `down` 后网络被回收），仍指向已销毁的网络 ID。脚本已内置自愈，手动等价命令：

```powershell
docker compose --profile ext --profile flink --profile spark down --remove-orphans
docker network rm bigdata-project_default     # 只删本项目网络，不影响其他项目
docker compose --profile ext up -d
```

> 注意：Kafka / ES 未配命名卷，`down` 后其数据随容器消失，重启后由 `orders-gen` 重新攒数（约 2–3 分钟恢复）。

### 10.5 端口冲突

先 `./stop.ps1`；确认没有其他程序占用（常见的 8080 被本地 Web 服务、9200 被本机 ES 占用）；或修改 `docker-compose.yml` 中冒号左侧的宿主端口。

### 10.6 HBase 相关

- **`hbase-sink` 一直刷 `Could not connect to ('hbase', 9090)`**：Thrift 没起来。`hbase/init/start-with-thrift.sh` 负责 `start-hbase.sh` 后 `sleep 20` 再 `hbase thrift start -p 9090`；确认该脚本被挂载到 `/hbase-init/` 且 `command` 是 `["/bin/bash","/hbase-init/start-with-thrift.sh"]`（两个不含空格的参数，否则会被镜像 entrypoint 二次分词导致容器空转重启）。
- **Hive 建映射表报 `Can't get connection to ZooKeeper`**：`hive/conf/hbase-site.xml` 没挂进 Hive 容器，或其中的 `hbase.zookeeper.quorum` 不等于 compose 服务名 `hbase`。该文件不能出现连续两个减号的注释分隔线，否则 XML 解析直接失败。
- **Hive 查 `ods.orders_hbase` 返回 0**：见 8.4 的坑，改用 `group by` 或 `limit`。

### 10.7 Elasticsearch / Kibana 反复重启

99% 是 Docker 内存不足。把 Docker Desktop 内存调到 10 GB（基础链路）或 12 GB 以上（含 Flink/Spark）；也可以先把 Flink/Spark 停掉只跑基础链路。

### 10.8 构建时报 buildx 错误（`parsing time ... extra text`）

本机 buildx 状态异常，用传统构建器绕过：

```powershell
$env:DOCKER_BUILDKIT=0
docker compose --profile ext up -d --build
```

### 10.9 Flink / Spark 起不来

- 内存：Flink JobManager 900M + TaskManager 1200M，Spark 1200M，容易触及总上限。
- Flink 作业提交失败时看 `docker logs flink-submit`；若提示 `未找到作业 jar`，说明 `flink/flink-orders-job.jar` 缺失，需先执行 `./flink/build.sh`。
- `flink/lib/` 下三个 jar 必须挂进容器的 `/opt/flink/lib`（JobManager、TaskManager、submit 三处都要），否则作业提交时报找不到 Kafka 连接器类。

### 10.10 想完全重来（会清空所有数据）

```powershell
docker compose --profile ext --profile flink --profile spark down -v
docker compose --profile ext up -d
```

`-v` 会删除 HDFS / HBase 的命名卷（`namenode_data`、`datanode_data`、`hbase_data`），NameNode 需要重新格式化，首次启动会慢一些。

---

## 11. 项目亮点（相对"教学 demo"做了哪些生产化改造）

这部分是"为什么不只是一个 demo"的证据，均为代码里真实存在的做法：

1. **补齐了原方案缺失的造数环节**。很多同类方案执行 `up` 之后 Kafka 里没有任何数据，后面所有图都是空的。本项目用 `orders-gen` 持续造数，并对每条订单定义了完整、稳定的字段契约（6 品类、3 状态加权、`amount` 自动计算、带时区的 `order_time`）。
2. **Logstash 拆成两条独立 pipeline**（`logstash/config/pipelines.yml`）。合并成一条会让两个 Kafka input 各消费一遍全量数据，且 `remove_field` 会互相污染字段；拆开后 `es` 与 `hdfs` 各用独立消费组（`logstash-es` / `logstash-hdfs`），互不干扰。
3. **显式索引模板，禁止 ES 自动推断字段类型**（`dynamic: strict` + 固定 6 个 `keyword`/数值类型 + 1 分片 0 副本 + `refresh_interval: 5s` + `total_fields.limit: 200`）。避免第一条脏数据把 `amount` 映射成 `text`、后续聚合全部失效。
4. **用业务时间覆盖 `@timestamp`**，并在容器内统一 `TZ=Asia/Shanghai`。保证 Kibana 时间轴与按天索引名都对应真实下单时间，而不是入库时间。
5. **ES 幂等写入**：`document_id => "%{order_id}"`，同一订单重复消费不会产生重复文档。
6. **HDFS 落盘按"天分区 + 小时文件"组织**（`/data/orders/dt=.../orders-HH.csv`），配合 Hive `PARTITIONED BY (dt)` 外部表 + `MSCK REPAIR TABLE` 自动注册分区，扫表只读需要的分区。
7. **Hive-on-HBase 的客户端配置显式注入**（`hive/conf/hbase-site.xml` 挂进 `hive-server` 与 `hive-init`）。不挂就会出现"建表成功但查询卡死"这类新手最难排查的问题。
8. **HBase 自启 Thrift**（`hbase/init/start-with-thrift.sh`）。官方镜像只起 master/regionserver，而 Python 侧的 happybase 走 9090 端口；脚本还规避了镜像 entrypoint `exec $@` 的二次分词坑（`command` 只传两个不含空格的参数）。
9. **写库可靠性**：`hbase-sink` 每 100 条批量 `put`，**先 `batch.send()` 落库、再 `consumer.commit()` 提交位点**，异常自动重建连接并重试，不静默丢消息。
10. **初始化自动化、可重复执行**。`hive-init` 把原来"手工进容器粘贴 SQL"变成一次性容器：等 HiveServer2 → 建表（全部 `IF NOT EXISTS`，反复执行不报错不动数据）→ 等分区 → 注册 → 校验行数 → 退出。`start.ps1` 还会自动导入 Kibana 索引模式与看板，省掉手工点界面。
11. **验收脚本区分"本次结果"和"历史残留"**。`verify.ps1` 会读 `spark-batch` 的结束时间，只有 30 分钟内以 `ExitCode=0` 完成的运行才算通过，避免"目录还在就放行"的假通过；Flink 也要求确实存在 `RUNNING` 作业。
12. **资源与日志治理**。每个服务都设 `deploy.resources.limits.memory`（防止某个容器吃光内存把宿主机拖死），日志统一 `json-file` 且 `max-size: 10m / max-file: 3`（长时间挂机不会把磁盘写满）。JVM 堆上限集中在 `.env` 统一调参。
13. **生产化 Kafka 参数**：3 分区（与 Logstash `consumer_threads: 3` 对齐，避免消费者空转或分区无人消费）、`lz4` 压缩、72 小时保留、128MB 段大小、`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`（加快演示启动）。
14. **健康检查驱动的启动顺序**。`depends_on` 全部配 `condition: service_healthy`，并且 HBase 的健康检查同时要求"shell 能跑"和"9090 在监听"，保证 `hbase-sink` 启动时 thrift 一定可用。
15. **一次性容器跑完即退**（`restart: "no"`）：`hive-init`、`flink-submit`、`spark-batch` 不长期占内存，符合"批任务"语义。
16. **两个计算引擎各司其职**：Flink 用**事件时间 + 水印**（容忍 5 秒乱序、30 秒空闲分区）做实时窗口聚合，结果双写（日志便于验收 + Kafka 便于下游消费）；Spark 用 `local[2]`、`shuffle.partitions=4` 在单机小数据量下也能跑得动，并用 WebHDFS 轮询解决"数据是异步到达"的时序问题。

---

## 12. 速查表：端口 / 常用命令 / 停止与清理

### 12.1 端口速查

| 服务 | 宿主机地址 | 说明 |
|---|---|---|
| Kibana | http://localhost:5601 | 链路 A 看板 |
| Elasticsearch | http://localhost:9200 | 链路 A 存储 |
| Kafka UI | http://localhost:8080 | topic `orders` / `orders_flink_agg` |
| NameNode | http://localhost:9870 | 浏览 `/data/orders`、`/data/spark` |
| HiveServer2 | jdbc:hive2://localhost:10000 | SQL 入口（Web UI 10002） |
| HBase Master | http://localhost:16010 | Thrift RPC 在 9090 |
| Kafka | localhost:29092 | 宿主机访问用 29092（容器内为 `kafka:9092`） |
| Logstash API | http://localhost:9600 | 管线状态与统计 |
| Flink Web UI | http://localhost:8081 | 需 `--profile flink` |
| Spark Master | http://localhost:8082 | 需 `--profile spark` |
| Spark 应用 UI | http://localhost:4040 | 仅 `spark-batch` 运行期间 |
| Redis | localhost:6379 | ext |

### 12.2 常用命令

```powershell
# 启动 / 停止 / 重启
.\start.ps1                        # 基础链路
.\start.ps1 -Stage all             # 全起（含 Flink + Spark）
.\stop.ps1                         # 停止（保留数据卷）
docker compose --profile ext restart logstash

# 看状态与日志
docker compose --profile ext ps
docker logs -f logstash
docker logs -f hbase-sink
docker logs -f hive-init

# 数据抽查
curl -s "http://localhost:9200/orders-*/_count"
docker exec namenode hdfs dfs -ls /data/orders
docker exec hbase bash -c "echo \"count 'orders'\" | hbase shell -n"
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "select count(1) from ods.orders_raw;"

# 重新跑一次扩展
docker compose --profile spark up -d --force-recreate spark-batch
docker compose --profile flink up -d --force-recreate flink-submit

# 一键验收
.\verify.ps1
.\verify.ps1 -Ext
```

### 12.3 停止与清理策略

| 操作 | 命令 | 数据是否保留 |
|---|---|---|
| 暂停服务 | `.\stop.ps1`（= `docker compose ... down`） | HDFS / HBase 数据在命名卷里，保留；Kafka / ES 数据随容器消失 |
| 彻底清零 | `docker compose --profile ext down -v` | 命名卷一并删除，全部重新开始 |

日常开发建议只用 `stop.ps1`，需要干净重来时再用 `down -v`。

---

### 附：一句话回顾

> **Kafka** 是传送带，**Logstash** 是分拣员，**ES + Kibana** 把数据变成看板，**HDFS + Hive** 把数据变成可查询的表格，**HBase** 提供按订单号的极速点查，**Flink** 算"每分钟"，**Spark** 算"这一整天"，**Docker Compose** 负责把这一整套东西一键装进你的电脑。
