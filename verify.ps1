<#
  一键验收：检查三条基础链路 + Flink/Spark 扩展链路。
  关键点：扩展链路会校验结果是否来自"本次运行"，历史遗留产出不再算通过。

  用法：
    .\verify.ps1         仅检查（不启动任何东西；扩展未启动或数据陈旧则显示[待查]）
    .\verify.ps1 -Ext    一键起 Spark 与 Flink 扩展，真跑一遍后立即验收（推荐）
#>
[CmdletBinding()]
param([switch]$Ext)

Set-Location -Path $PSScriptRoot
$ProgressPreference = 'SilentlyContinue'
$stamp = Get-Date
# 扩展结果有效窗口：窗口内跑过的 Spark 批分析算通过，超出则视为旧运行残留
$FreshMinutes = 30

function Line($name, $ok, $detail) {
  if ($ok) { Write-Host ("[通过] {0} —— {1}" -f $name, $detail) -ForegroundColor Green }
  else     { Write-Host ("[待查] {0} —— {1}" -f $name, $detail) -ForegroundColor Yellow }
}

# 把 docker 返回的 UTC ISO 时间转成本地 DateTime
function ToLocal([string]$iso) {
  if ([string]::IsNullOrWhiteSpace($iso)) { return $null }
  $s = $iso.Substring(0, [Math]::Min(19, $iso.Length)).Replace('T', ' ')
  try {
    $dt = [datetime]::ParseExact($s, 'yyyy-MM-dd HH:mm:ss', $null)
    $dt = [datetime]::SpecifyKind($dt, [DateTimeKind]::Utc)
    return [System.TimeZoneInfo]::ConvertTimeFromUtc($dt, [System.TimeZoneInfo]::Local)
  } catch { return $null }
}

# 取容器状态时间
function ContainerRun([string]$cname) {
  $raw = docker inspect $cname --format "{{.State.Status}}|{{.State.ExitCode}}|{{.State.StartedAt}}|{{.State.FinishedAt}}" 2>$null
  if (-not $raw) { return $null }
  $p = ($raw | Out-String).Trim().Split('|')
  if ($p.Count -lt 4) { return $null }
  [pscustomobject]@{
    Status   = $p[0]
    ExitCode = $p[1]
    Started  = ToLocal $p[2]
    Finished = ToLocal $p[3]
  }
}

Write-Host "================ 验收开始 ================" -ForegroundColor Cyan
Write-Host ("检查时间：{0}" -f $stamp.ToString('yyyy-MM-dd HH:mm:ss')) -ForegroundColor DarkGray

# ---------- 扩展链路：一键启动与真跑 ----------
if ($Ext) {
  Write-Host "`n---- 阶段一：重跑 Spark 批分析（一次性容器，跑完即退出以释放内存）----" -ForegroundColor Cyan
  docker compose --profile spark stop spark 2>$null | Out-Null      # 常驻 Master 不参与，避免抢内存
  docker compose --profile spark rm -f spark-batch 2>$null | Out-Null
  docker compose --profile spark up -d spark-batch 2>$null | Out-Null
  $t0 = Get-Date
  while (((Get-Date) - $t0).TotalSeconds -lt 420) {
    Start-Sleep -Seconds 5
    $sb = ContainerRun 'spark-batch'
    if ($sb -and $sb.Status -eq 'exited') { break }
  }
  $sb = ContainerRun 'spark-batch'
  if ($sb -and $sb.Status -eq 'exited') {
    Write-Host ("  spark-batch 已结束，ExitCode={0}，耗时 {1:N0} 秒" -f $sb.ExitCode, ($sb.Finished - $sb.Started).TotalSeconds) -ForegroundColor DarkGray
  } else {
    Write-Host "  spark-batch 仍在运行，验收时按当前状态判定；可另开窗口 docker logs -f spark-batch 观察" -ForegroundColor DarkGray
  }

  Write-Host "`n---- 阶段二：启动 Flink 流作业（常驻）----" -ForegroundColor Cyan
  docker compose --profile flink up -d 2>$null | Out-Null
  $t0 = Get-Date
  while (((Get-Date) - $t0).TotalSeconds -lt 210) {
    $j = try { (Invoke-RestMethod -Uri "http://127.0.0.1:8081/jobs/overview" -TimeoutSec 5).jobs } catch { $null }
    if ($j -and @($j | Where-Object { $_.state -eq 'RUNNING' }).Count -gt 0) { break }
    Start-Sleep -Seconds 8
  }
  Write-Host "  Flink 启动阶段结束" -ForegroundColor DarkGray
}

Write-Host "`n---- 验收明细 ----" -ForegroundColor Cyan
$results = New-Object System.Collections.ArrayList
function AddResult($ok) { [void]$results.Add([bool]$ok) }

# 1. 容器状态
$ps = docker compose --profile ext --profile flink --profile spark ps --format "{{.Name}}`t{{.State}}" 2>$null
$running = @($ps | Where-Object { $_ -match 'running' }).Count
$total = @($ps | Where-Object { $_ -match '\S' }).Count
Line "容器运行数" ($running -ge 13) "$running 个 running / 共 $total 个（基础链路 14 个，起 Flink 后 16 个）"
AddResult ($running -ge 13)

# 2. 链路 A：Elasticsearch 入库
try {
  $es = Invoke-RestMethod -Uri "http://127.0.0.1:9200/orders-*/_count" -TimeoutSec 10
  Line "链路A ES 文档数" ($es.count -gt 0) "orders-* 共 $($es.count) 条"
  AddResult ($es.count -gt 0)
} catch { Line "链路A ES 文档数" $false "无法访问 127.0.0.1:9200，可能仍在启动，等 1-2 分钟再试"; AddResult $false }

# 3. 链路 B：HDFS 落盘
$hdfs = docker exec namenode hdfs dfs -ls /data/orders 2>$null | Out-String
$part = ($hdfs -match "dt=")
Line "链路B HDFS 分区" $part $(if ($part) { "已出现 dt= 分区目录" } else { "尚未出现分区，Logstash 可能还在攒数据" })
AddResult $part

# 4. 链路 B：Hive 外部表
$raw = docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent --outputformat=tsv2 -e "select count(1) from ods.orders_raw;" 2>$null | Out-String
$rawNum = ($raw -split "`n" | Where-Object { $_ -match '^\s*\d+\s*$' } | Select-Object -First 1)
Line "链路B Hive orders_raw" ($null -ne $rawNum) $(if ($rawNum) { "行数 $($rawNum.Trim())" } else { "查询无返回，检查 hive-init 是否 Exited 0" })
AddResult ($null -ne $rawNum)

# 5. 链路 C：HBase 行数
$hb = docker exec hbase bash -c "echo \`"count 'orders'\`" | hbase shell -n" 2>$null | Out-String
$hbNum = ([regex]::Matches($hb, '(\d+)\s+row\(s\)') | Select-Object -Last 1)
if ($hbNum) { $hbOk = ([int]$hbNum.Groups[1].Value -gt 0); Line "链路C HBase orders 表" $hbOk "$($hbNum.Groups[1].Value) 行"; AddResult $hbOk }
else { Line "链路C HBase orders 表" $false "未取到行数，执行 docker exec hbase bash -c ""echo \""count 'orders'\"" | hbase shell -n"" 复查"; AddResult $false }

# 6. 链路 C：Hive 映射表
$mapq = docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent --outputformat=tsv2 -e "select category,count(1) c from ods.orders_hbase group by category;" 2>$null | Out-String
$hasCat = ($mapq -match "electronics|clothing|food|books|home|beauty|sports")
Line "链路C Hive 映射表" $hasCat $(if ($hasCat) { "按品类分组有返回" } else { "无分组结果，用 group by 查（勿用 count(1)，会走元数据返回 0）" })
AddResult $hasCat

# 7. 扩展 D：Flink —— 必须是本次启动后仍在 RUNNING 的作业
$jobs = try { (Invoke-RestMethod -Uri "http://127.0.0.1:8081/jobs/overview" -TimeoutSec 5).jobs } catch { $null }
if ($null -eq $jobs) {
  Line "扩展D Flink 作业" $false "127.0.0.1:8081 无响应，Flink 未启动（运行 .\verify.ps1 -Ext 一键起）"
  AddResult $false
} else {
  $runJobs = @($jobs | Where-Object { $_.state -eq 'RUNNING' })
  $jm = ContainerRun 'flink-jobmanager'
  $jmUp = if ($jm -and $jm.Started) { "，JobManager 启动于 $($jm.Started.ToString('HH:mm:ss'))（已运行 {0:N0} 分钟）" -f ($stamp - $jm.Started).TotalMinutes } else { "" }
  if ($runJobs.Count -gt 0) {
    $names = ($runJobs | ForEach-Object { $_.name }) -join ', '
    Line "扩展D Flink 作业" $true "$($runJobs.Count) 个 RUNNING：$names$jmUp"
    AddResult $true
  } else {
    Line "扩展D Flink 作业" $false "JobManager 在线但无 RUNNING 作业，查看 docker logs flink-submit$jmUp"
    AddResult $false
  }
}

# 8. 扩展 E：Spark —— 4 个指标目录必须齐，且必须是近期有效运行产出（不接受历史残留）
$sp = docker exec namenode hdfs dfs -ls /data/spark 2>$null | Out-String
$need = @('category_stats', 'daily_stats', 'hourly_stats', 'top_users')
$missing = @($need | Where-Object { $sp -notmatch $_ })
$sb = ContainerRun 'spark-batch'
$ageMin = if ($sb -and $sb.Finished) { [math]::Round(($stamp - $sb.Finished).TotalMinutes, 1) } else { $null }
$sbInfo = if ($sb -and $sb.Finished) { "spark-batch 末次结束于 $($sb.Finished.ToString('HH:mm:ss'))，距今 $ageMin 分钟" } else { "尚未运行过 spark-batch" }

if ($missing.Count -gt 0) {
  $msg = if ($null -eq $sb) { "尚未跑过 spark-batch；执行 docker compose --profile spark up -d spark-batch 后看日志" }
         else { "缺少 $($missing -join ', ')；$sbInfo" }
  Line "扩展E Spark 输出" $false $msg
  AddResult $false
}
elseif (($null -eq $sb) -or ($null -eq $sb.Finished) -or ($ageMin -gt $FreshMinutes)) {
  Line "扩展E Spark 输出" $false "4 个目录虽在，但属旧运行残留（$sbInfo，超出 $FreshMinutes 分钟有效窗口），不代表本次结果；重跑：docker compose --profile spark up -d --force-recreate spark-batch"
  AddResult $false
}
elseif ($sb.ExitCode -ne '0') {
  Line "扩展E Spark 输出" $false "近期跑过但 ExitCode=$($sb.ExitCode)，看 docker logs spark-batch 定位"
  AddResult $false
}
else {
  Line "扩展E Spark 输出" $true "4 个 Parquet 指标目录齐全，末次运行 ExitCode=0（$($sb.Finished.ToString('HH:mm:ss')) 完成，距今 $ageMin 分钟）"
  AddResult $true
}

# ---------- 汇总 ----------
$pass = @($results | Where-Object { $_ }).Count
$all = $results.Count
Write-Host "`n==========================================" -ForegroundColor Cyan
if ($pass -eq $all) {
  Write-Host ("验收结果：{0}/{1} 全部通过，基础链路与扩展链路均为本次运行有效产出。" -f $pass, $all) -ForegroundColor Green
} else {
  Write-Host ("验收结果：{0}/{1} 通过，{2} 项待查。" -f $pass, $all, ($all - $pass)) -ForegroundColor Yellow
  Write-Host "  一键让扩展链路真跑一遍： .\verify.ps1 -Ext" -ForegroundColor Yellow
  Write-Host "  仅想复跑 Spark 批分析：  docker compose --profile spark up -d --force-recreate spark-batch; docker logs -f spark-batch" -ForegroundColor Yellow
  Write-Host "  仅想复起 Flink 流作业：  docker compose --profile flink up -d; 打开 http://localhost:8081 看 RUNNING" -ForegroundColor Yellow
}
Write-Host ("提示：基础链路数据靠 orders-gen/logstash 持续写入，攒够数需等 2-3 分钟；扩展链路结果只在 {0} 分钟窗口内算有效，历史目录不算通过。" -f $FreshMinutes) -ForegroundColor DarkGray
Write-Host "      验收完毕想释放内存： docker compose --profile flink down" -ForegroundColor DarkGray
