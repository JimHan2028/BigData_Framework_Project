#!/usr/bin/env bash
# 一键验收：三条基础链路 + Flink/Spark 扩展链路。
# 关键点：扩展结果会校验"是否为近期真实运行产出"，历史残留不算通过。
#
# 用法：
#   ./verify.sh          仅检查（不启动任何东西）
#   ./verify.sh --ext    一键起 Spark 与 Flink 扩展，真跑一遍后立即验收（推荐）
cd "$(dirname "$0")"
EXT=0
case "${1:-}" in -e|--ext) EXT=1 ;; esac

ok(){ printf "\033[32m[通过] %s\033[0m\n" "$1"; }
pend(){ printf "\033[33m[待查] %s\033[0m\n" "$1"; }
info(){ printf "\033[90m%s\033[0m\n" "$1"; }

STAMP=$(date '+%Y-%m-%d %H:%M:%S')
FRESH_MIN=30
PASS=0; ALL=0
mark(){ ALL=$((ALL+1)); [ "$1" = "1" ] && PASS=$((PASS+1)); }

echo "================ 验收开始 ================"
info "检查时间：$STAMP"

if [ "$EXT" = "1" ]; then
  echo
  echo "---- 阶段一：重跑 Spark 批分析（一次性容器，跑完即退出以释放内存）----"
  docker compose --profile spark stop spark >/dev/null 2>&1
  docker compose --profile spark rm -sf spark-batch >/dev/null 2>&1
  docker compose --profile spark up -d spark-batch >/dev/null 2>&1
  for _ in $(seq 1 84); do
    ST=$(docker inspect spark-batch --format '{{.State.Status}}' 2>/dev/null)
    [ "$ST" = "exited" ] && break
    sleep 5
  done
  info "  spark-batch 状态：${ST:-unknown}"

  echo "---- 阶段二：启动 Flink 流作业（常驻）----"
  docker compose --profile flink up -d >/dev/null 2>&1
  for _ in $(seq 1 26); do
    curl -s --max-time 5 http://localhost:8081/jobs/overview | grep -q '"state":"RUNNING"' && break
    sleep 8
  done
  info "  Flink 启动阶段结束"
fi

echo
echo "---- 验收明细 ----"

RUN=$(docker compose --profile ext --profile flink --profile spark ps 2>/dev/null | grep -c "Up")
if [ "$RUN" -ge 13 ]; then ok "容器运行数 —— $RUN 个 running（基础链路 14 个，起 Flink 后 16 个）"; mark 1
else pend "容器运行数 —— 仅 $RUN 个 running，预期至少 13"; mark 0; fi

C=$(curl -s --max-time 10 "http://localhost:9200/orders-*/_count" | grep -o '"count":[0-9]*' | head -1 | cut -d: -f2)
if [ -n "${C:-}" ] && [ "${C:-0}" -gt 0 ]; then ok "链路A ES 文档数 —— orders-* 共 $C 条"; mark 1
else pend "链路A ES 文档数 —— 无数据（可能仍在启动，等 1-2 分钟）"; mark 0; fi

if docker exec namenode hdfs dfs -ls /data/orders 2>/dev/null | grep -q "dt="; then ok "链路B HDFS 分区 —— 已出现 dt= 分区目录"; mark 1
else pend "链路B HDFS 分区 —— 尚未出现分区"; mark 0; fi

RAW=$(docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent --outputformat=tsv2 -e "select count(1) from ods.orders_raw;" 2>/dev/null | tr -d '[:space:]' | grep -E '^[0-9]+$' | head -1)
if [ -n "${RAW:-}" ]; then ok "链路B Hive orders_raw —— 行数 $RAW"; mark 1
else pend "链路B Hive orders_raw —— 查询无返回"; mark 0; fi

HB=$(docker exec hbase bash -c "echo \"count 'orders'\" | hbase shell -n" 2>/dev/null | grep -o '[0-9]* row(s)' | tail -1)
if [ -n "${HB:-}" ]; then ok "链路C HBase orders 表 —— $HB"; mark 1
else pend "链路C HBase orders 表 —— 未取到行数"; mark 0; fi

if docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent --outputformat=tsv2 \
     -e "select category,count(1) c from ods.orders_hbase group by category;" 2>/dev/null \
     | grep -qE "electronics|clothing|food|books|home|beauty|sports"; then
  ok "链路C Hive 映射表 —— 按品类分组有返回"; mark 1
else pend "链路C Hive 映射表 —— 无分组结果（勿用 count(1)，会走元数据返回 0）"; mark 0; fi

JOBS=$(curl -s --max-time 5 http://localhost:8081/jobs/overview 2>/dev/null)
if [ -z "${JOBS:-}" ]; then
  pend "扩展D Flink 作业 —— 127.0.0.1:8081 无响应，Flink 未启动（运行 ./verify.sh --ext 一键起）"; mark 0
elif echo "$JOBS" | grep -q '"state":"RUNNING"'; then
  ok "扩展D Flink 作业 —— 至少 1 个 RUNNING 作业"; mark 1
else
  pend "扩展D Flink 作业 —— JobManager 在线但无 RUNNING 作业，看 docker logs flink-submit"; mark 0
fi

HAS=0
SP=$(docker exec namenode hdfs dfs -ls /data/spark 2>/dev/null)
MISS=""
for d in category_stats daily_stats hourly_stats top_users; do
  if echo "$SP" | grep -q "$d"; then HAS=$((HAS+1)); else MISS="$MISS $d"; fi
done
FIN=$(docker inspect spark-batch --format '{{.State.FinishedAt}}' 2>/dev/null)
EXIT=$(docker inspect spark-batch --format '{{.State.ExitCode}}' 2>/dev/null)
AGE=""
if [ -n "${FIN:-}" ] && [ "${FIN:0:4}" != "0001" ]; then
  if FINS=$(date -u -d "$FIN" +%s 2>/dev/null); then
    AGE=$(( ( $(date -u +%s) - FINS ) / 60 ))
  elif command -v python3 >/dev/null 2>&1; then
    AGE=$(python3 -c "import datetime,sys;print(int((datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat('${FIN%Z}+00:00')).total_seconds()//60))" 2>/dev/null)
  fi
fi

if [ "$HAS" -lt 4 ]; then
  pend "扩展E Spark 输出 —— 缺少:$MISS（末次结束 ${FIN:-无}）"; mark 0
elif [ -z "${AGE:-}" ] || [ "${AGE:-99999}" -gt "$FRESH_MIN" ]; then
  pend "扩展E Spark 输出 —— 4 个目录虽在，但属旧运行残留（末次结束 ${FIN:-未知}，距今约 ${AGE:-?} 分钟），重跑：docker compose --profile spark up -d --force-recreate spark-batch"; mark 0
elif [ "${EXIT:-1}" != "0" ]; then
  pend "扩展E Spark 输出 —— 近期跑过但 ExitCode=$EXIT，看 docker logs spark-batch"; mark 0
else
  ok "扩展E Spark 输出 —— 4 个 Parquet 指标目录齐全，末次运行 ExitCode=0（距今 ${AGE:-0} 分钟）"; mark 1
fi

echo
echo "=========================================="
if [ "$PASS" = "$ALL" ]; then
  printf "\033[32m验收结果：%d/%d 全部通过，基础链路与扩展链路均为近期有效产出。\033[0m\n" "$PASS" "$ALL"
else
  printf "\033[33m验收结果：%d/%d 通过，%d 项待查。\033[0m\n" "$PASS" "$ALL" "$((ALL-PASS))"
  echo "  一键让扩展链路真跑一遍： ./verify.sh --ext"
  echo "  仅复跑 Spark 批分析：    docker compose --profile spark up -d --force-recreate spark-batch; docker logs -f spark-batch"
  echo "  仅复起 Flink 流作业：    docker compose --profile flink up -d; 打开 http://localhost:8081 看 RUNNING"
fi
info "提示：基础链路需 2-3 分钟攒数；扩展结果只在 ${FRESH_MIN} 分钟窗口内算有效，验证完可 docker compose --profile flink down 释放内存。"
