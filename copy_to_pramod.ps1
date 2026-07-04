Write-Host ""
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host " Azure Course - Copy Instructor Content" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ""

$created = 0
$skipped = 0

Get-ChildItem -Directory | Where-Object {
    $_.Name -like "day_*"
} | ForEach-Object {

    $source = $_.FullName
    $target = Join-Path ".\pramod" $_.Name

    if (!(Test-Path $target)) {

        Copy-Item $source $target -Recurse

        Write-Host "Created: $($_.Name)" -ForegroundColor Green

        $created++

    }
    else {

        Write-Host "Skipped: $($_.Name) already exists" -ForegroundColor Yellow

        $skipped++

    }

}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
Write-Host "Created : $created"
Write-Host "Skipped : $skipped"
Write-Host ""