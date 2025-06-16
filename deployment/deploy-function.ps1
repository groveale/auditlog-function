# Azure Function Deployment Script
# Save as: deploy-function.ps1

param(
    [string]$ResourceGroupName = "rg-gamification-test",
    [string]$Location = "uksouth",
    [string]$SourceCodePath = "../src",
    [string]$BicepFilePath = "./main.bicep",
    [string]$ApplicationName = "gamification",
    [string]$Environment = "test",
    [string]$EncryptionKey = "<your-encryption-key-here>", # Should be a secure value
    [string]$TenantId = "<your-tenant-id>",
    [string]$AuthGuid = "<your-auth-guid>",
    [string]$SPO_SiteId = "<your-spo-site-id>",
    [string]$SPO_ListId = "<your-spo-list-id>"
)

# Function to write colored output
function Write-Status {
    param([string]$Message, [string]$Color = "Green")
    Write-Host "[INFO] $Message" -ForegroundColor $Color
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Step {
    param([string]$Message)
    Write-Host "[STEP] $Message" -ForegroundColor Cyan
}

# Set error action preference
$ErrorActionPreference = "Stop"

try {
    # Validation
    Write-Step "Validating prerequisites..."
    
    if (-not (Test-Path $BicepFilePath)) {
        Write-Error "Bicep file not found at: $BicepFilePath"
        exit 1
    }

    if (-not (Test-Path $SourceCodePath)) {
        Write-Error "Source code directory not found at: $SourceCodePath"
        exit 1
    }

    $csprojFiles = Get-ChildItem -Path $SourceCodePath -Filter "*.csproj" -Recurse
    if ($csprojFiles.Count -eq 0) {
        Write-Error "No .csproj file found in: $SourceCodePath"
        exit 1
    }

    # Check if Azure CLI is installed
    try {
        az --version | Out-Null
    }
    catch {
        Write-Error "Azure CLI is not installed or not in PATH"
        exit 1
    }

    # Check if .NET CLI is installed
    try {
        dotnet --version | Out-Null
    }
    catch {
        Write-Error ".NET CLI is not installed or not in PATH"
        exit 1
    }

    # Check Azure login status
    Write-Status "Checking Azure login status..."
    try {
        $account = az account show | ConvertFrom-Json
        Write-Status "Logged in as: $($account.user.name)"
    }
    catch {
        Write-Warning "Not logged in to Azure. Please login..."
        az login
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Azure login failed"
            exit 1
        }
    }

    # Step 1: Deploy Infrastructure
    Write-Step "Deploying infrastructure with Bicep..."
    
    Write-Status "Creating resource group: $ResourceGroupName"
    az group create --name $ResourceGroupName --location $Location --output table
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create resource group"
        exit 1
    }

    Write-Status "Deploying Bicep template..."
    $deploymentOutput = az deployment group create `
        --resource-group $ResourceGroupName `
        --template-file $BicepFilePath `
        --parameters `
        location=$Location `
        runtime=dotnet-isolated `
        applicationName=$ApplicationName `
        environment=$Environment `
        encryptionKey="$EncryptionKey" `
        tenantId=$TenantId `
        authGuid=$AuthGuid `
        spoSiteId=$SPO_SiteId `
        spoListId=$SPO_ListId `
        --query 'properties.outputs' `
        --output json | ConvertFrom-Json

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Infrastructure deployment failed!"
        exit 1
    }

    Write-Status "Infrastructure deployed successfully!"

    # Extract deployment outputs
    $deployedFunctionName = $deploymentOutput.functionAppName.value
    $hostname = $deploymentOutput.functionAppHostName.value

    # Managed identity details
    $managedIdentityObjectId = $deploymentOutput.functionAppPrincipalId.value


    # Connect to Graph

    try {
        # Connect to Microsoft Graph - as a global admin or with sufficient permissions
        Write-Step "Connecting to Microsoft Graph..."
        Connect-MgGraph -NoWelcome -Scopes `
            "Sites.FullControl.All", `
            "Application.Read.All", `
            "AppRoleAssignment.ReadWrite.All"

        # Variables (make sure these are set before running the script)
        $principalId = $managedIdentityObjectId        # 👈 Must be the OBJECT ID of the service principal

        # Get clientId from principalId 👈 Used for SharePoint site grant
        $sp = Get-MgServicePrincipal -ServicePrincipalId $principalId
        $clientId = $sp.AppId
      
        $displayName = $deployedFunctionName
        $siteId = $SPO_SiteId                          # Format: "yourtenant.sharepoint.com,site-guid,web-guid"


        $existingAssignments = Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $principalId

        # Helper function to check if role is already assigned
        function Test-RoleAssigned($roleId, $resourceId, $assignments) {
            return $assignments | Where-Object {
                $_.AppRoleId -eq $roleId -and $_.ResourceId -eq $resourceId
            }
        }

        ### 1. Assign ActivityFeed.Read from Office 365 Management APIs ###
        $managementAppId = "c5393580-f805-4401-95e8-94b7a6ef2fc2"
        $managementSp = Get-MgServicePrincipal -Filter "appId eq '$managementAppId'"
        $activityFeedRole = $managementSp.AppRoles | Where-Object {
            $_.Value -eq "ActivityFeed.Read" -and $_.AllowedMemberTypes -contains "Application"
        }

        if ($activityFeedRole -and -not (Test-RoleAssigned $activityFeedRole.Id $managementSp.Id $existingAssignments)) {
            $newRole = New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $principalId `
                -PrincipalId $principalId `
                -ResourceId $managementSp.Id `
                -AppRoleId $activityFeedRole.Id
        }


        ### 2. Assign Sites.Selected from Microsoft Graph ###
        $graphAppId = "00000003-0000-0000-c000-000000000000"
        $graphSp = Get-MgServicePrincipal -Filter "appId eq '$graphAppId'"
        $sitesSelectedRole = $graphSp.AppRoles | Where-Object {
            $_.Value -eq "Sites.Selected" -and $_.AllowedMemberTypes -contains "Application"
        }
        if ($sitesSelectedRole -and -not (Test-RoleAssigned $sitesSelectedRole.Id $graphSp.Id $existingAssignments)) {
            $newRole = New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $principalId `
                -PrincipalId $principalId `
                -ResourceId $graphSp.Id `
                -AppRoleId $sitesSelectedRole.Id
        }
        

        # Assign ReportSettings.ReadWrite.All
        $reportSettingsRole = $graphSp.AppRoles | Where-Object {
            $_.Value -eq "ReportSettings.ReadWrite.All" -and $_.AllowedMemberTypes -contains "Application"
        }
        if ($reportSettingsRole -and -not (Test-RoleAssigned $reportSettingsRole.Id $graphSp.Id $existingAssignments)) {
            $newRole = New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $principalId `
                -PrincipalId $principalId `
                -ResourceId $graphSp.Id `
                -AppRoleId $reportSettingsRole.Id
        }

        # Assign Reports.Read.All
        $reportsReadAllRole = $graphSp.AppRoles | Where-Object {
            $_.Value -eq "Reports.Read.All" -and $_.AllowedMemberTypes -contains "Application"
        }
        if ($reportsReadAllRole -and -not (Test-RoleAssigned $reportsReadAllRole.Id $graphSp.Id $existingAssignments)) {
            $newRole = New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $principalId `
                -PrincipalId $principalId `
                -ResourceId $graphSp.Id `
                -AppRoleId $reportsReadAllRole.Id
        }


        ### 3. Grant SharePoint permission using Sites.Selected model ###
        $permissionBody = @{
            roles               = @("read")  # or "write"
            grantedToIdentities = @(
                @{
                    application = @{
                        id          = $clientId       # Must be CLIENT ID here, not objectId
                        displayName = $displayName
                    }
                }
            )
        }

        $newSPOPerms = New-MgSitePermission -SiteId $siteId -BodyParameter $permissionBody
    }
    catch {
        Write-Error "❌ Failed to assign permissions to the managed identity: $($_.Exception.Message)"
        exit 1
    }


    Write-Status "Managed identity permissions assigned successfully!"

    # Step 2: Build .NET Function
    Write-Step "Building .NET function..."
    
    $originalLocation = Get-Location
    Set-Location $SourceCodePath

    try {
        # Clean and restore
        Write-Status "Cleaning and restoring packages..."
        dotnet clean
        if ($LASTEXITCODE -ne 0) {
            throw "dotnet clean failed"
        }

        dotnet restore
        if ($LASTEXITCODE -ne 0) {
            throw "dotnet restore failed"
        }

        # Build in Release mode
        Write-Status "Building in Release mode..."
        dotnet build --configuration Release --no-restore
        if ($LASTEXITCODE -ne 0) {
            throw ".NET build failed"
        }

        # Step 3: Publish .NET Function
        Write-Step "Publishing .NET function..."
        $publishDir = "./bin/Release/publish"
        
        if (Test-Path $publishDir) {
            Remove-Item $publishDir -Recurse -Force
        }

        dotnet publish --configuration Release --output $publishDir --no-build --verbosity minimal
        if ($LASTEXITCODE -ne 0) {
            throw ".NET publish failed"
        }

        # Step 4: Create deployment package
        Write-Step "Creating deployment package..."
        $zipPath = "./bin/Release/deploy.zip"
        
        if (Test-Path $zipPath) {
            Remove-Item $zipPath -Force
        }

        # Use PowerShell's Compress-Archive
        Compress-Archive -Path "$publishDir/*" -DestinationPath $zipPath -Force
        
        if (-not (Test-Path $zipPath)) {
            throw "Failed to create deployment package"
        }

        Write-Status "Deployment package created: $zipPath"

    }
    finally {
        Set-Location $originalLocation
    }

    # Step 5: Deploy to Azure Function
    Write-Step "Deploying code to Azure Function..."
    
    $zipFullPath = Resolve-Path "$SourceCodePath/bin/Release/deploy.zip"
    
    az functionapp deployment source config-zip `
        --resource-group $ResourceGroupName `
        --name $deployedFunctionName `
        --src $zipFullPath `
        --timeout 300

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Code deployment failed!"
        exit 1
    }

    # Step 6: Wait for deployment and verify
    Write-Step "Verifying deployment..."
    Start-Sleep -Seconds 15

    # Check if function is running
    Write-Status "Checking function app status..."
    $status = az functionapp show `
        --resource-group $ResourceGroupName `
        --name $deployedFunctionName `
        --query 'state' `
        --output tsv

    if ($status -eq "Running") {
        Write-Status "Function app is running successfully!"
    }
    else {
        Write-Warning "Function app status: $status"
    }

    # Step 7: Get function information
    Write-Step "Retrieving deployment information..."

    # Get the master key
    Write-Status "Retrieving function keys..."
    try {
        $masterKey = az functionapp keys list `
            --resource-group $ResourceGroupName `
            --name $deployedFunctionName `
            --query 'masterKey' `
            --output tsv 2>$null
        
        if ([string]::IsNullOrEmpty($masterKey)) {
            $masterKey = "Unable to retrieve"
        }
    }
    catch {
        $masterKey = "Unable to retrieve"
    }

    # Get function URLs
    Write-Status "Retrieving function information..."
    try {
        $functions = az functionapp function list `
            --resource-group $ResourceGroupName `
            --name $deployedFunctionName `
            --query '[].{name:name}' `
            --output json | ConvertFrom-Json
    }
    catch {
        $functions = @()
    }

    # Step 8: Output summary
    Write-Step "Deployment Summary"
    Write-Host "==================================" -ForegroundColor Cyan
    Write-Status "Resource Group: $ResourceGroupName"
    Write-Status "Function App Name: $deployedFunctionName"
    Write-Status "Function App URL: https://$hostname"
    Write-Status "Master Key: $masterKey"
    Write-Host ""
    
    if ($functions.Count -gt 0) {
        Write-Status "Available Functions:"
        foreach ($func in $functions) {
            Write-Host "  - $($func.name)" -ForegroundColor White
        }
    }
    else {
        Write-Status "No HTTP triggered functions found"
    }
    
    Write-Host ""
    Write-Step "Setting Up Audit Subscription via PowerShell"
    Write-Host "==================================" -ForegroundColor Cyan

    $webhookAddressURLEncoded = [System.Web.HttpUtility]::UrlEncode("https://$hostname/api/ReceiveEvents")

    $subscriptionUrl = "https://$hostname/api/SubscribeToEvent?contentType=Audit.General&webhookAddress=$webhookAddressURLEncoded"

    $response = Invoke-RestMethod -Uri $subscriptionUrl -Method Post
    Write-Host "Subscription setup response:"
    Write-Host $response

    Write-Host "`nYou can now test Copilot interactions are coming through by triggering an event in M365 Copilot"

    Write-Host ""

    Write-Host "`n=== Test Copilot Interactions are coming through ==="
    Write-Host "1. Please wait 5 minutes to allow the event to be processed (from your Copilot interaction testing)."
    Write-Host "2. Visit the following URL to check the results:"
    Write-Host "   https://$hostname/api/GetNotifications"
    Write-Host ""
    Write-Host "If you do not see your interaction, something has gone wrong. Please check the Azure Function logs for errors."

    # Clean up
    Write-Step "Cleaning up temporary files..."
    $cleanupPath = "$SourceCodePath/bin/Release/deploy.zip"
    if (Test-Path $cleanupPath) {
        Remove-Item $cleanupPath -Force
    }

    Write-Host ""
    Write-Status "Deployment completed successfully! 🎉" "Green"

}
catch {
    Write-Error "Deployment failed: $($_.Exception.Message)"
    Write-Host "Stack trace:" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}