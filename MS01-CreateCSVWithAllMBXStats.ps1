function write-log
{
    param(
        [parameter(mandatory=$true, position=0)]
        $message,
        [parameter(mandatory=$false, position=1)]
        [string][ValidateSet('error', 'info', 'info2', 'warning', 'ok')] $type,
        [parameter(mandatory=$false, position=2)]
        [switch] $silent,
        [parameter(mandatory=$false, position=3)]
        [switch] $skipTimestamp
    )

    if($null -eq $message){
        $message = ""
    }
    $message = ($message | Out-String).trim()

    try {
        if(-not $skipTimestamp){
            $message = "$(Get-Date -Format "hh:mm:ss>")" + $type.ToUpper() + ": " + $message
        }

        Add-Content -Path $logFile -Value $message

        if(-not $silent){
            switch($type){
                'error'{
                    write-host -ForegroundColor Red $message
                }
                'info'{
                    write-host -ForegroundColor DarkGray $message
                }
                'info2'{
                    write-host -ForegroundColor Blue $message
                }
                'warning'{
                    write-host -ForegroundColor Yellow $message
                }
                'ok'{
                    write-host -ForegroundColor Green $message
                }
                default{
                    write-host $message
                }

            }
        }
    }

    catch{
        Write-Host "Not able to write to log. Suggest ro cncel the script run."
        $_
    }
}

# logfile stuff
$date = get-date -Format "yyyyMMdd_HHmm"
$currentPath = ((Get-Location).path) + "\"
$logFilesPath = $currentPath + "LogFiles\"

if(!(Test-Path $logFilesPath))
{
    New-Item -Path $currentPath -Name "LogFiles" -ItemType Directory | 
        Out-Null
}

$logFileName = "$($date)_MigrationBatches"
$logFile = $logFilesPath+$logFileName+".log"

New-Item -Path $logFilesPath -Name "Databases" -ItemType Directory -ErrorAction SilentlyContinue | 
    Out-Null

write-log "Directory for logs created" -type info
write-log "Collecting mailboxes. Processing..." -type info

$csv = @()

# collect all mbx from organization
$mailboxes = get-mailbox -resultsize unlimited -warningaction silentlycontinue -erroraction silentlycontinue
$numberOfMbx = $mailboxes | 
    measure | 
        select -ExpandProperty count

write-log "Number of mailboxes: $numberOfMbx" -type info
write-log "Collecting statistics of mailboxes. Processing..." -type info

$counterProgress = 0

# collect stats totalitemsize, itemcount
foreach($mbx in $mailboxes)
{
    $progressPercent = [int]($counterProgress/$numberOfMbx * 100)
    
    # process bar
    Write-progress -Activity "Collecting statistics of mailboxes" -Status "$($counterProgress)/$numberOfMbx" -PercentComplete $progressPercent
    
    $stats = $mbx |  
        get-mailboxstatistics -erroraction silentlycontinue -warningaction silentlycontinue

    # if user didnt log on his mailbox anymore, the script shows an error.
    # To avoid it, lets assign zeros to totalitemsize, itemcount
    if($null -eq $stats)
    {
        $csv += New-Object psobject -Property @{
            DisplayName   = $mbx.displayname
            GUID          = $mbx.guid
            TotalItemSize = 0
            ItemCount     = 0
        }    
    }
    
    # comment

    # bigint is needed to sort variables as numbers, not strings
    else
    {
        $csv += New-Object psobject -Property @{
            DisplayName   = $mbx.displayname
            GUID          = $mbx.guid
            TotalItemSize = [bigint]$stats.totalitemsize.value.tobytes()
            ItemCount     = [int]$stats.itemcount
        }
    }

    $counterProgress ++
}

write-log "Stats collected." -type ok

# select to put in order
$csv | 
    select DisplayName,GUID,TotalItemSize,ItemCount | 
        Export-Csv -Path ".\$($date)_AllMBXWithStats.csv" -Delimiter ";" -NoTypeInformation -Encoding UTF8
