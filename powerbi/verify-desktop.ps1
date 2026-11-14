param([string]$OutputPath = 'work/phase5-powerbi-model.json', [string]$BusinessId = '')
# Consulta SOLO LECTURA al modelo local abierto en Power BI Desktop.
$ErrorActionPreference = 'Stop'
$taskPackage = Get-AppxPackage Microsoft.MicrosoftPowerBIDesktop
if (-not $taskPackage) { throw 'Power BI Desktop Store no está instalado.' }
Add-Type -Path (Join-Path $taskPackage.InstallLocation 'bin/Microsoft.PowerBI.AdomdClient.dll')
$taskEngines = @(Get-CimInstance Win32_Process -Filter "Name='msmdsrv.exe'")
if ($taskEngines.Count -ne 1) { throw 'Abre únicamente el informe LifeMap en Power BI Desktop.' }
if ($taskEngines[0].CommandLine -notmatch '-s\s+"([^"]+)"') { throw 'No se encontró el workspace local.' }
$taskWorkspace = $Matches[1]
$taskPorts = @(Get-ChildItem -LiteralPath $taskWorkspace -Filter 'msmdsrv.port.txt')
if ($taskPorts.Count -ne 1) { throw 'Puerto local ambiguo.' }
$taskPort = (Get-Content -LiteralPath $taskPorts[0].FullName -Encoding Unicode).Trim()
if ($taskPort -notmatch '^\d+$') { throw 'Puerto inválido.' }
$taskConnection = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$taskPort;")
try {
    $taskConnection.Open()
    $taskCommand = $taskConnection.CreateCommand()
    $taskCommand.CommandText = 'EVALUATE ROW("alerts",COUNTROWS(Alerts),"hotspots",COALESCE(COUNTROWS(Hotspots),0),"km_points",COUNTROWS(KaplanMeier),"evolution_points",COUNTROWS(Evolution),"loaded_at",MAX(''Refresh''[loaded_at_utc]),"api_at",MAX(''Refresh''[api_generated_at]))'
    if ($BusinessId) {
        if ($BusinessId -notmatch '^[A-Za-z0-9_-]{1,128}$') { throw 'Identificador inválido.' }
        $taskCommand.CommandText=$taskCommand.CommandText.Substring(0,$taskCommand.CommandText.Length-1)+(',"selected_score",MAXX(FILTER(Alerts,Alerts[business_id]="{0}"),Alerts[risk_score]),"selected_event",MAXX(FILTER(Alerts,Alerts[business_id]="{0}"),Alerts[event_date]))' -f $BusinessId)
    }
    $taskReader = $taskCommand.ExecuteReader()
    try {
        if (-not $taskReader.Read()) { throw 'No se obtuvo la fila de verificación.' }
        $taskResult = @{}
        for ($taskIndex=0; $taskIndex -lt $taskReader.FieldCount; $taskIndex++) {
            $taskValue = $taskReader.GetValue($taskIndex)
            if ($taskValue -is [datetime]) { $taskValue=$taskValue.ToString('yyyy-MM-ddTHH:mm:ss.fff')+'Z' }
            $taskResult[$taskReader.GetName($taskIndex).Trim('[',']')]=$taskValue
        }
        $taskJson = $taskResult | ConvertTo-Json -Compress
        $taskJson | Set-Content -LiteralPath $OutputPath -Encoding UTF8
        $taskJson
    } finally { $taskReader.Close() }
} finally { $taskConnection.Close() }
