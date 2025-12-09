# Deploy script for GhostKing Protocol update
Write-Host "=== GC_EDGE Deployment Script ===" -ForegroundColor Green
Set-Location "G:\My Drive\GCedge"

Write-Host "`n1. Checking git status..." -ForegroundColor Yellow
git status

Write-Host "`n2. Adding all changes..." -ForegroundColor Yellow  
git add -A

Write-Host "`n3. Committing changes..." -ForegroundColor Yellow
git commit -m "Add GhostKing Protocol module for ES1! macro regime analysis"

Write-Host "`n4. Pushing to origin..." -ForegroundColor Yellow
git push origin main

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "Railway will automatically deploy from GitHub." -ForegroundColor Cyan

