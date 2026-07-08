Clear-Host

Write-Host ""
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "      Azure Data Engineering Course Sync Utility" -ForegroundColor Cyan
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

# Create pramod folder if missing
if (!(Test-Path ".\pramod")) {
    New-Item -Path ".\pramod" -ItemType Directory | Out-Null
}

Get-ChildItem -Directory | Where-Object {
    $_.Name -like "day_*"
} | ForEach-Object {

    $sourceFolder = $_.FullName
    $targetFolder = Join-Path ".\pramod" $_.Name

    # Create day folder if it doesn't exist
    if (!(Test-Path $targetFolder)) {

        New-Item -Path $targetFolder -ItemType Directory -Force | Out-Null

        Write-Host ""
        Write-Host "Created folder: $($_.Name)" -ForegroundColor Green

        $createdFolders++
    }

    # Copy all files recursively
    Get-ChildItem -Path $sourceFolder -Recurse -File | ForEach-Object {

        # Skip ignored folders
        $skip = $false

        foreach ($ignore in $ignoreFolders) {
            if ($_.FullName -match "\\$ignore\\") {
                $skip = $true
                break
            }
        }

        if ($skip) {
            return
        }

        $relativePath = $_.FullName.Substring($sourceFolder.Length).TrimStart('\')

        $destination = Join-Path $targetFolder $relativePath

        $destinationFolder = Split-Path -Parent $destination

        # Create nested folders if needed
        if (!(Test-Path $destinationFolder)) {
            New-Item -Path $destinationFolder -ItemType Directory -Force | Out-Null
        }

        # Copy only if file doesn't exist
        if (!(Test-Path $destination)) {

            Copy-Item -Path $_.FullName -Destination $destination

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