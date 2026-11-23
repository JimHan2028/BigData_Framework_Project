<#
  电商订单实时分析全栈 · 一键启动（Windows / PowerShell 5.1+）
  用法：
    .\start.ps1               基础链路（A/B/C 三条链路 + kafka-ui + redis）
    .\start.ps1 -Stage flink  在基础链路之上再拉起 Flink
    .\start.ps1 -Stage spark  在基础链路之上再拉起 Spark
    .\start.ps1 -Stage all    一次全起（内存需 12GB 以上）
#>
param(
  [ValidateSet('core','flink','spark','all')]
  [string]$Stage = 'core'
)

function Info($m){ Write-Host "[步骤] $m" -ForegroundColor Cyan }
function Ok($m)  { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[警告] $m" -ForegroundColor Yellow }
function Die($m) { Write-Host "[失败] $m" -ForegroundColor Red; exit 1 }

Set-Location -Path $PSScriptRoot

# ---------- 1. 前置检查 ----------
Info "检查 Docker 是否已启动"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
  Die "Docker 未运行。请先打开 Docker Desktop，等状态变成 running 后再执行本脚本。"
}
docker compose version *> $null
if ($LASTEXITCODE -ne 0) {
  Die "未找到 docker compose。需要 Docker Desktop 4.x 及以上版本（自带 Compose V2）。"
}
Ok "Docker 与 Compose 就绪"

$memGB = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
Info "本机物理内存 ${memGB}GB"
if ($memGB -lt 12) {
  Warn "内存低于 12GB，同时启动 Flink/Spark 可能触发 OOM，建议只跑基础链路（默认即基础链路）。"
}

# ---------- 2. 端口占用检查 ----------
$ports = @(8080, 9200, 5601, 10000, 2181, 9870, 9000, 16010, 9092)
if ($Stage -eq 'flink' -or $Stage -eq 'all') { $ports += 8081 }
if ($Stage -eq 'spark' -or $Stage -eq 'all') { $ports += @(8082, 4040) }
$busy = @()
foreach ($p in $ports) {
  if (Get-NetTCPConnection -State Listen -LocalPort $p -ErrorAction SilentlyContinue) { $busy += $p }
}
if ($busy.Count -gt 0) {
  # 区分「本项目容器已在运行」（幂等重跑，应放行）与「外部进程占用」（需提示）
  $selfNames = @(docker compose --profile ext --profile flink --profile spark ps --all --format "{{.Name}}" 2>$null)
  $selfPorts = @()
  if ($selfNames.Count -gt 0) {
    foreach ($line in @(docker compose --profile ext --profile flink --profile spark ps --all --format "{{.Ports}}" 2>$null)) {
      foreach ($m in [regex]::Matches($line, ':(\d+)->')) { $selfPorts += [int]$m.Groups[1].Value }
    }
  }
  $external = @($busy | Where-Object { $selfPorts -notcontains $_ })
  if ($external.Count -eq 0) {
    Info ("端口均由本项目容器占用（" + ($busy -join ', ') + "），判定为重复启动：执行幂等增补，不会重建容器或丢数据")
  }
  else {
    Warn ("以下端口被非本项目进程占用：" + ($external -join ', '))
    Warn "常见原因是本机已装占用同端口的软件，或旧容器不属于本 compose 项目。可先执行 .\stop.ps1 再试。"
    $ans = Read-Host "是否继续启动？(y/N)"
    if ($ans -ne 'y' -and $ans -ne 'Y') { Write-Host "已取消"; exit 0 }
  }
}

# ---------- 3. 按需拉取镜像（本地已有则完全跳过，不访问 registry） ----------
Info "检查所需镜像"
# bde2020/hive 的 pinned tag 已被作者从 Docker Hub 下架，同内容仅剩 latest（digest 相同），缺失时用 latest 打回原名
$hiveImg = 'bde2020/hive:2.3.1-postgresql-metastore'
docker image inspect $hiveImg *> $null
if ($LASTEXITCODE -ne 0) {
  Warn "$hiveImg 本地缺失，且该 tag 已在 Docker Hub 下架（报 no such manifest）"
  Info "改用同内容的 bde2020/hive:latest 并打回原 tag，无需改配置"
  docker pull bde2020/hive:latest
  if ($LASTEXITCODE -eq 0) {
    docker tag bde2020/hive:latest $hiveImg
    Ok "已用 latest 补位为 $hiveImg（两者 digest 一致，行为完全相同）"
  }
  else { Warn "补位失败：请检查网络后重跑，或更换镜像加速器" }
}
$needImages = @()
foreach ($img in @(docker compose --profile ext --profile flink --profile spark config --images 2>$null)) {
  if ($img -and $img -notmatch '^bigdata-project-') { $needImages += $img }
}
$needImages = @($needImages | Sort-Object -Unique)
$missing = @()
foreach ($img in $needImages) {
  docker image inspect $img *> $null
  if ($LASTEXITCODE -ne 0) { $missing += $img }
}
if ($missing.Count -eq 0) {
  Ok ("本地已有所需的 " + $needImages.Count + " 个镜像，跳过拉取（不联网校验，不会出现镜像加速器报错）")
}
else {
  Warn ("缺少 " + $missing.Count + " 个镜像，开始拉取（首次总量约 13GB，视网速 10-30 分钟）：")
  $missing | ForEach-Object { Write-Host ("        - " + $_) -ForegroundColor DarkGray }
  docker compose pull
  if ($LASTEXITCODE -ne 0) {
    Warn "部分镜像未拉取成功。最常见原因是镜像加速器不含该仓库（bde2020/* 与 docker.elastic.co/* 命中率最低）"
    Warn "若报错镜像本地已存在，可直接忽略；否则换一个加速器后重跑本脚本，或单独 docker pull 该镜像"
  }
}

# ---------- 4. 启动基础链路 ----------
Info "构建 3 个自建镜像并启动基础链路（orders-gen / hbase-sink / logstash）"
$upLog = (docker compose --profile ext up -d --build 2>&1 | Out-String)
if ($LASTEXITCODE -ne 0) {
  if ($upLog -match 'network\s+\S+\s+not found' -or $upLog -match 'failed to set up container networking') {
    Warn "检测到容器引用了已失效的 Docker 网络（常见于 Docker Desktop 重启、网络被重建或上次 down 未彻底之后）"
    Info "自愈处理：删除并重建全部容器（数据卷保留，HDFS / HBase 数据不丢；Kafka / ES 无命名卷，将重新攒数）"
    docker compose --profile ext --profile flink --profile spark down --remove-orphans
    docker network rm bigdata-project_default 2>$null | Out-Null   # 仅删本项目网络，不影响其他项目
    $upLog = (docker compose --profile ext up -d --build 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) {
      Write-Host $upLog
      Die "自愈失败。请手动执行：docker compose --profile ext --profile flink --profile spark down --remove-orphans，再重跑本脚本。"
    }
    Ok "已重建容器并恢复网络"
  }
  else {
    Write-Host $upLog
    Die "启动失败，请查看上方日志（完整日志：docker compose logs --tail=100）"
  }
}
Ok "基础链路已启动（13 个容器 + kafka-ui + redis）"

# ---------- 5. 等待建表完成 ----------
Info "等待 hive-init 完成建表（首次 2-5 分钟，最长等 10 分钟）"
$deadline = (Get-Date).AddMinutes(10)
$done = $false
while ((Get-Date) -lt $deadline) {
  Start-Sleep -Seconds 10
  $st = docker inspect -f '{{.State.Status}}' hive-init 2>$null
  if ($st -eq 'exited') {
    $code = docker inspect -f '{{.State.ExitCode}}' hive-init 2>$null
    if ("$code".Trim() -eq '0') { Ok "hive-init 已完成建表（Exited 0，正常现象）" }
    else { Warn "hive-init 退出码 $code，请执行 docker logs hive-init 查看原因" }
    $done = $true
    break
  }
}
if (-not $done) { Warn "hive-init 10 分钟内未结束，可稍后执行 docker logs -f hive-init 观察" }

# ---------- 5.5 导入 Kibana saved objects（索引模式 + Dashboard） ----------
$rootDir = $PSScriptRoot
if (-not $rootDir) { $rootDir = (Get-Location).Path }
$kbnFile = Join-Path $rootDir 'kibana\saved_objects.ndjson'
if (Test-Path $kbnFile) {
  Info "等待 Kibana 就绪，导入索引模式与 Dashboard"
  $kbnOk = $false
  for ($i = 0; $i -lt 30; $i++) {
    try { $null = Invoke-RestMethod -Uri 'http://127.0.0.1:5601/api/status' -TimeoutSec 5; $kbnOk = $true; break }
    catch { Start-Sleep -Seconds 5 }
  }
  if ($kbnOk) {
    try {
      $boundary = [System.Guid]::NewGuid().ToString()
      $bytes = [System.IO.File]::ReadAllBytes($kbnFile)
      $head = "--$boundary`r`nContent-Disposition: form-data; name=`"file`"; filename=`"saved_objects.ndjson`"`r`nContent-Type: application/x-ndjson`r`n`r`n"
      $tail = "`r`n--$boundary--`r`n"
      $ms = New-Object System.IO.MemoryStream
      $hb = [System.Text.Encoding]::UTF8.GetBytes($head); $ms.Write($hb, 0, $hb.Length)
      $ms.Write($bytes, 0, $bytes.Length)
      $tb = [System.Text.Encoding]::UTF8.GetBytes($tail); $ms.Write($tb, 0, $tb.Length)
      $res = Invoke-RestMethod -Uri 'http://127.0.0.1:5601/api/saved_objects/_import?overwrite=true' -Method Post `
             -Headers @{ 'kbn-xsrf' = 'true' } -ContentType "multipart/form-data; boundary=$boundary" `
             -Body $ms.ToArray() -TimeoutSec 60
      if ($res.success) { Ok "Kibana 已导入索引模式 orders* 与 Dashboard「电商订单实时分析」" }
      else { Warn "Kibana 部分对象导入失败，可在 Kibana 手工导入 kibana\saved_objects.ndjson" }
    } catch { Warn "Kibana 导入失败（不影响主链路），可手工导入 kibana\saved_objects.ndjson" }
  } else { Warn "Kibana 150 秒内未就绪，跳过导入（不影响主链路）" }
}

# ---------- 6. 可选扩展引擎 ----------
if ($Stage -eq 'flink' -or $Stage -eq 'all') {
  Info "启动 Flink 并提交实时窗口聚合作业"
  docker compose --profile flink up -d flink-jobmanager flink-taskmanager
  docker compose --profile flink up flink-submit
  Ok "Flink 已提交（flink-submit 退出属正常）"
}
if ($Stage -eq 'spark' -or $Stage -eq 'all') {
  Info "启动 Spark 并跑一次离线批分析"
  docker compose --profile spark up -d spark
  docker compose --profile spark up spark-batch
  Ok "Spark 批分析已执行（spark-batch 退出属正常）"
}

# ---------- 7. 访问入口 ----------
Write-Host ""
Write-Host "================ 访问入口 ================" -ForegroundColor Cyan
Write-Host "  Kibana           http://localhost:5601"
Write-Host "  Kafka UI         http://localhost:8080"
Write-Host "  HDFS NameNode    http://localhost:9870"
Write-Host "  HBase Master     http://localhost:16010"
Write-Host "  Elasticsearch    http://localhost:9200"
Write-Host "  HiveServer2      jdbc:hive2://localhost:10000"
if ($Stage -eq 'flink' -or $Stage -eq 'all') { Write-Host "  Flink Web UI     http://localhost:8081" }
if ($Stage -eq 'spark' -or $Stage -eq 'all') { Write-Host "  Spark Master     http://localhost:8082  应用 UI http://localhost:4040（仅作业运行期间）" }
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Info "下一步：.\verify.ps1 自动验收；.\verify.ps1 -Ext 连 Flink/Spark 扩展一起真跑一遍；.\stop.ps1 停止（保留数据卷）"
Info "数据 2-3 分钟后才会在 Kibana / Hive 中出现，属正常现象"
