Clear-Host

Write-Host ""
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "      Azure Data Engineering Course Sync Utility"
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host ""

$createdFolders = 0
$createdFiles   = 0
$skippedFiles   = 0

$ignoreFolders = @(
    ".git",
    ".ipynb_checkpoints",
    "__pycache__",
    "pramod"
)

Get-ChildItem -Directory | Where-Object {
    $_.Name -like "day_*"
} | ForEach-Object {

    $sourceFolder = $_.FullName
    $targetFolder = Join-Path ".\pramod" $_.Name

    if (!(Test-Path $targetFolder)) {

        New-Item -ItemType Directory -Path $targetFolder -Force | Out-Null

        Write-Host ""
        Write-Host "Created folder: $($_.Name)" -ForegroundColor Green

        $createdFolders++
    }

    Get-ChildItem $sourceFolder -Recurse | ForEach-Object {

        if ($_.PSIsContainer) {
            return
        }

        foreach ($ignore in $ignoreFolders) {
            if ($_.FullName -like "*\$ignore\*") {
                return
            }
        }

        $relativePath = $_.FullName.Substring($sourceFolder.Length).TrimStart('\')

        $destination = Join-Path $targetFolder $relativePath

        $destinationFolder = Split-Path $destination

        if (!(Test-Path $destinationFolder)) {
            New-Item -ItemType Directory -ItemType Directory -Force -Path $destinationFolder | Out-Null
        }

        if (!(Test-Path $destination)) {

            Copy-Item $_.FullName $destination

            Write-Host "  + $relativePath" -ForegroundColor Green

            $createdFiles++
        }
        else {

            $skippedFiles++

        }

    }

}

Write-Host ""
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Summary" -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

Write-Host "New folders : $createdFolders" -ForegroundColor Green
Write-Host "New files   : $createdFiles" -ForegroundColor Green
Write-Host "Skipped     : $skippedFiles" -ForegroundColor Yellow

Write-Host ""
Write-Host "Instructor content synchronized successfully." -ForegroundColor Cyan
Write-Host ""