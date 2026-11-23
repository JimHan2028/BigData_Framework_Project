#!/usr/bin/env bash
# 编译 Flink 作业（生成 flink/flink-orders-job.jar）
#
# 说明：flink 官方镜像只带 JRE，没有 javac，因此借 eclipse-temurin:11-jdk 镜像编译，
#       编译用的 Flink 依赖 jar 直接从 flink 镜像的 /opt/flink/lib 导出，无需本机装 JDK/Maven。
#
# 用法：bash flink/build.sh [eclipse-temurin:11-jdk 或其它 JDK 镜像]
set -euo pipefail
cd "$(dirname "$0")"

FLINK_IMAGE="flink:1.17.2-scala_2.12-java11"
JDK_IMAGE="${1:-eclipse-temurin:11-jdk}"

LIB_DIR="$(mktemp -d)"
trap 'rm -rf "$LIB_DIR"' EXIT

echo "[build] 从 $FLINK_IMAGE 导出 Flink 依赖库 ..."
docker create --name flink-lib-tmp "$FLINK_IMAGE" >/dev/null
docker cp flink-lib-tmp:/opt/flink/lib "$LIB_DIR/lib" >/dev/null
docker rm -f flink-lib-tmp >/dev/null

echo "[build] 编译源码（JDK 镜像: $JDK_IMAGE）..."
docker run --rm \
  -v "$LIB_DIR/lib:/opt/flink/lib:ro" \
  -v "$PWD/lib:/opt/extra-lib:ro" \
  -v "$PWD:/app" \
  -w /app \
  "$JDK_IMAGE" \
  bash -c "rm -rf build/classes && mkdir -p build/classes \
        && javac -encoding UTF-8 -cp '/opt/flink/lib/*:/opt/extra-lib/*' -d build/classes \$(find src -name '*.java') \
        && jar cf flink-orders-job.jar -C build/classes ."

echo "[build] 完成：$PWD/flink-orders-job.jar"
