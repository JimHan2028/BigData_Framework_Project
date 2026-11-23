<# 停止全部容器，保留数据卷（数据不丢）。如需彻底清空数据，见 DELIVERY.md 第 6 节。 #>
Set-Location -Path $PSScriptRoot
Write-Host "[步骤] 停止全部容器（不加 -v，数据卷保留）" -ForegroundColor Cyan
docker compose --profile ext --profile flink --profile spark down
Write-Host "[ OK ] 已停止。数据仍在 Docker 卷中，再次 start.ps1 可直接继续。" -ForegroundColor Green
