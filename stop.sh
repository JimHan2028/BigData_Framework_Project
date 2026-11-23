#!/usr/bin/env bash
# 停止全部容器，保留数据卷（数据不丢）
cd "$(dirname "$0")"
docker compose --profile ext --profile flink --profile spark down
printf "\033[32m[ OK ] 已停止。数据仍在 Docker 卷中，再次 ./start.sh 可继续。\033[0m\n"
