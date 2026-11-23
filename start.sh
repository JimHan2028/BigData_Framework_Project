#!/usr/bin/env bash
# 电商订单实时分析全栈 · 一键启动（macOS / Linux）
# 用法：
#   ./start.sh              基础链路（A/B/C 三条链路 + kafka-ui + redis）
#   ./start.sh flink        在基础链路之上再拉起 Flink
#   ./start.sh spark        在基础链路之上再拉起 Spark
#   ./start.sh all          一次全起（内存需 12GB 以上）
set -u

STAGE="${1:-core}"
cd "$(dirname "$0")"

info(){ printf "\033[36m[步骤] %s\033[0m\n" "$1"; }
ok(){   printf "\033[32m[ OK ] %s\033[0m\n" "$1"; }
warn(){ printf "\033[33m[警告] %s\033[0m\n" "$1"; }
die(){  printf "\033[31m[失败] %s\033[0m\n" "$1"; exit 1; }

info "检查 Docker 是否已启动"
docker info >/dev/null 2>&1 || die "Docker 未运行。请先启动 Docker Desktop 后再执行。"
docker compose version >/dev/null 2>&1 || die "未找到 docker compose（需 Docker Compose V2）。"
ok "Docker 与 Compose 就绪"

PORTS="8080 9200 5601 10000 2181 9870 9000 16010 9092"
[ "$STAGE" = "flink" ] || [ "$STAGE" = "all" ] && PORTS="$PORTS 8081"
[ "$STAGE" = "spark" ] || [ "$STAGE" = "all" ] && PORTS="$PORTS 8082 4040"
BUSY=""
for p in $PORTS; do
  if command -v lsof >/dev/null 2>&1 && lsof -iTCP:"$p" -sTCP:LISTEN -t >/dev/null 2>&1; then BUSY="$BUSY $p"; fi
done
[ -n "$BUSY" ] && warn "以下端口已被占用：$BUSY（若为上次未退出的容器，可先执行 ./stop.sh）"

info "预拉取镜像（首次约 13GB，视网速 10-30 分钟）"
# bde2020/hive 的 pinned tag 已从 Docker Hub 下架，同内容仅剩 latest，缺失时打回原名
HIVE_IMG="bde2020/hive:2.3.1-postgresql-metastore"
if ! docker image inspect "$HIVE_IMG" >/dev/null 2>&1; then
  warn "$HIVE_IMG 本地缺失，且该 tag 已在 Docker Hub 下架，改用同内容 latest 补位"
  if docker pull bde2020/hive:latest; then
    docker tag bde2020/hive:latest "$HIVE_IMG" && ok "已用 latest 补位为 $HIVE_IMG"
  else
    warn "补位失败：请检查网络后重跑"
  fi
fi

# 按需拉取：本地已有则完全跳过，不访问 registry
NEED=$(docker compose --profile ext --profile flink --profile spark config --images 2>/dev/null | grep -v '^bigdata-project-' | sort -u)
MISSING=""
for img in $NEED; do
  docker image inspect "$img" >/dev/null 2>&1 || MISSING="$MISSING $img"
done
if [ -z "$MISSING" ]; then
  ok "本地已有所需镜像，跳过拉取（不联网校验）"
else
  warn "缺少以下镜像，开始拉取（首次总量约 13GB，10-30 分钟）：$MISSING"
  docker compose pull || warn "部分镜像未拉取成功。最常见原因是加速器不含该仓库（bde2020/*、docker.elastic.co/*）。若该镜像本地已存在可忽略，否则换加速器后重跑"
fi

info "构建 3 个自建镜像并启动基础链路"
if ! UPLOG=$(docker compose --profile ext up -d --build 2>&1); then
  if echo "$UPLOG" | grep -qE "network .* not found|failed to set up container networking"; then
    warn "检测到容器引用了已失效的 Docker 网络（常见于 Docker Desktop 重启或网络被重建后）"
    info "自愈处理：删除并重建全部容器（数据卷保留；Kafka/ES 无命名卷，将重新攒数）"
    docker compose --profile ext --profile flink --profile spark down --remove-orphans
    docker network rm bigdata-project_default >/dev/null 2>&1 || true   # 仅删本项目网络
    docker compose --profile ext up -d --build || { echo "$UPLOG"; die "自愈失败，请手动执行 docker compose --profile ext --profile flink --profile spark down --remove-orphans 后重试"; }
    ok "已重建容器并恢复网络"
  else
    echo "$UPLOG"
    die "启动失败，请查看上方日志（docker compose logs --tail=100）"
  fi
fi
ok "基础链路已启动"

info "等待 hive-init 完成建表（首次 2-5 分钟，最长等 10 分钟）"
for i in $(seq 1 60); do
  sleep 10
  ST=$(docker inspect -f '{{.State.Status}}' hive-init 2>/dev/null || echo "")
  if [ "$ST" = "exited" ]; then
    CODE=$(docker inspect -f '{{.State.ExitCode}}' hive-init 2>/dev/null || echo "?")
    [ "$CODE" = "0" ] && ok "hive-init 已完成建表（Exited 0，正常现象）" || warn "hive-init 退出码 $CODE，请执行 docker logs hive-init 查看"
    break
  fi
done

# 导入 Kibana saved objects（索引模式 + Dashboard）
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$ROOT_DIR/kibana/saved_objects.ndjson" ]; then
  info "等待 Kibana 就绪，导入索引模式与 Dashboard"
  for i in $(seq 1 30); do
    curl -s --max-time 5 http://127.0.0.1:5601/api/status >/dev/null 2>&1 && break
    sleep 5
  done
  if curl -s -X POST "http://127.0.0.1:5601/api/saved_objects/_import?overwrite=true" \
       -H "kbn-xsrf: true" \
       -F "file=@$ROOT_DIR/kibana/saved_objects.ndjson" 2>/dev/null | grep -q '"success":true'; then
    ok "Kibana 已导入索引模式 orders* 与 Dashboard「电商订单实时分析」"
  else
    warn "Kibana 导入失败（不影响主链路），可手工导入 kibana/saved_objects.ndjson"
  fi
fi

case "$STAGE" in
  flink|all)
    info "启动 Flink 并提交实时窗口聚合作业"
    docker compose --profile flink up -d flink-jobmanager flink-taskmanager
    docker compose --profile flink up flink-submit
    ok "Flink 已提交（flink-submit 退出属正常）" ;;
esac
case "$STAGE" in
  spark|all)
    info "启动 Spark 并跑一次离线批分析"
    docker compose --profile spark up -d spark
    docker compose --profile spark up spark-batch
    ok "Spark 批分析已执行（spark-batch 退出属正常）" ;;
esac

cat <<'EOF'

================ 访问入口 ================
  Kibana           http://localhost:5601
  Kafka UI         http://localhost:8080
  HDFS NameNode    http://localhost:9870
  HBase Master     http://localhost:16010
  Elasticsearch    http://localhost:9200
  HiveServer2      jdbc:hive2://localhost:10000
  Flink Web UI     http://localhost:8081（启动 flink 时）
  Spark Master     http://localhost:8082（启动 spark 时）
==========================================

下一步：./verify.sh 自动验收；./stop.sh 停止（保留数据卷）
数据 2-3 分钟后才会在 Kibana / Hive 中出现，属正常现象
EOF
