param
(
    [parameter(mandatory=$true, position=0)]
    [string]$InputCSV = "",
    [parameter(mandatory=$false, position=2)]
    [int]$NumberOfBatches=10
)

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
write-log "Getting content from $InputCSV" -type info

$file = Get-Content $InputCSV

# checking if the file is in the right format.
# line 0 or 1, depends on the "notypeinformation" parameter value during exporting
if('"DisplayName";"GUID";"TotalItemSize";"ItemCount"' -in ($file[0], $file[1]))
{
    $csv = ConvertFrom-Csv $file -Delimiter ";"
}

else
{
    write-log "Incorrect file format." -type error
    write-log 'Looking for headers "DisplayName";"GUID";"TotalItemSize";"ItemCount"' -type error

    return -1
}

# changing objects type for all mbx
$csv |
    ForEach-Object {
        $_.totalitemsize = [bigint]$_.totalitemsize
        $_.itemcount     = [int]$_.itemcount
    }

$numberOfMbx = $csv | 
    measure | 
        select -ExpandProperty count

# sort by totalitemsize
$sorted = $csv | 
    select displayname, guid, totalitemsize, itemcount |
        sort totalitemsize -Descending

# export sorted mbx
$sorted | 
    Export-Csv -Path ".\$($date)_AllMBXSorted.csv" -Delimiter ";" -Force -Encoding UTF8 -NoTypeInformation

write-log "Mailboxes sorted by TotalItemSize exported to $($logFileName)_list_of_mbx.csv" -type ok

$sizeOfAllMbx = $sorted.totalitemsize | 
    measure -sum | 
        select -ExpandProperty sum

$batchAverageSize = $sizeOfAllMbx/$NumberOfBatches

write-log "Total size of all mailboxes: $([System.Math]::Round($sizeOfAllMbx/1024/1024,2)) MB" -type info2
write-log "Number of batches: $NumberOfBatches" -type info2
write-log "Batch average size: $([System.Math]::Round($batchAverageSize/1024/1024, 2)) MB" -type info2

$batch = @{}
$batchInfo = @()
$batchNames = @()

# creating a dictionary with names of batches as keys
for($i=1; $i -le $NumberOfBatches; $i++)
{
    $batchNames += "$($date)_MI_Migration_Batch$($i.ToString('000'))"
}

write-log "Batch names:" -type info
$batchNames | 
    foreach {
        write-log "$_" -type info -skipTimestamp
    }

# for every batch create empty array
foreach($batchName in $batchNames)
{
    $batch[$batchName] = @()
}

$i = $sorted.length - 1
$SumOfMbxInBatches = 0 # to check if all mbx are packed

# for every batch
foreach($batchName in $batchNames)
{
    $batchCurrentSize = 0

    # while batch size is lower than average size
    while($batchCurrentSize -lt $batchAverageSize)
    {
        if($i -lt 0)
        {
            break;
        }

        $batch[$batchName] += $sorted[$i]
        $batchCurrentSize += $($sorted[$i]).totalitemsize
        $i --
    }

    $numberOfMailboxesInBatch = $batch[$batchName] | 
        measure | 
            select -ExpandProperty count

    $totalSizeOfBatch = $batch[$batchName] | 
        select -ExpandProperty totalitemsize | 
            measure -sum | 
                select -ExpandProperty sum

    $totalItemsInMailboxes = $batch[$batchName] | 
        select -ExpandProperty itemcount | 
            measure -sum | 
                select -ExpandProperty sum

    write-log "Batch $batchName created." -type ok
    write-log "Number of mailboxes: $numberOfMailboxesInBatch" -type info
    write-log "Total size of batch: $([System.Math]::Round($totalSizeOfBatch/1024/1024,2)) MB" -type info
    write-log "Total items in mailboxes: $totalItemsInMailboxes" -type info

    $batchInfo += New-Object psobject -Property @{
        BatchName         = $batchName
        NumberOfMailboxes = $numberOfMailboxesInBatch
        TotalItemSize     = $totalSizeOfBatch
        ItemCount         = $totalItemsInMailboxes
    }

    $SumOfMbxInBatches += $numberOfMailboxesInBatch

    if($i -lt 0)
    {
        break;
    }
}

if($SumOfMbxInBatches -eq $numberOfMbx)
{
    Write-log "$SumOfMbxInBatches/$numberOfMbx mailboxes packed to batches." -type ok
}

else
{
    Write-log "$SumOfMbxInBatches/$numberOfMbx mailboxes packed to batches." -type error
}

# export batches to csv
foreach($batchName in $batchNames)
{
    $batch[$batchName] | 
        Export-Csv -Path "$logFilesPath$($logFileName)_$batchName.csv" -Delimiter ";" -Force -Encoding UTF8 -NoTypeInformation
}

# export batch info
$batchInfo | 
    Export-Csv -Path "$logFilesPath$($logFileName)_batches_info.csv" -Delimiter ";" -Force -Encoding UTF8 -NoTypeInformation


# divide mbx to specific mailbox databases
$names   = @()
$database = @{}

# create db names
foreach($j in 1..18)
{
    $names += "DB$($j.tostring("000"))"
}

foreach($n in $names)
{
    $database[$n] = @()
}

$counter = 0
foreach($batchName in $batchNames)
{
    $whichDB = @()
    
    foreach($mbx in $batch[$batchName])
    {
        # add mbx to specific db
        # first mbx to first db
        # second mbx to last db
        # third mbx to first+1 db
        # fourth mbx to last-1 db
        # fifth mbx to first+2 db
        # sixth mbx to last-2 db
        # ...
        # mbx are sorted so after the loop databases should have similar sizes.

        $dbName = ""
        if($($counter%48) % 2 -eq 0)
        {
            $dbName = "DB$(($([Math]::Floor($counter/2) % 48)+1).tostring("000"))"
        }
        else
        {
            $dbName = "DB$((48-$([Math]::Floor($counter/2) % 48)).tostring("000"))"
        }

        $database[$dbName] += $mbx
        $whichDB += New-Object psobject -property @{
            DisplayName   = $mbx.displayname
            GUID          = $mbx.guid
            TotalItemSize = $mbx.totalitemsize
            ItemCount     = $mbx.itemcount
            Database      = $dbName
        }

        $counter++
    }

    $whichDB = $whichDB | 
        select displayname, guid, totalitemsize, itemcount, database |
            sort totalitemsize

    # export to csv
    $whichDB | 
        Export-Csv -Path "$($logFilesPath)\Databases\$($logFileName)_$($batchname).csv" -Delimiter ";" -Force -Encoding UTF8 -NoTypeInformation 
}

# counting totalitemsize for every database
$dbSizes = @()
foreach($n in $names)
{
    $dbSize = $($database[$n].totalitemsize) | 
        measure -sum | 
            select -ExpandProperty sum
    $dbSizes += New-Object psobject -Property @{
        MailboxDatabase = $n
        TotalItemSize = $dbSize
    }

    write-log "$n - $([System.Math]::Round($dbSize/1024/1024,2)) MB `tafter the migration" -type info     
}

$dbSizes = $dbSizes | 
    select mailboxdatabase, totalitemsize

$dbSizes | 
    Export-Csv -Path "$($logFilesPath)\Databases\DB_sizes_after_migration.csv" -Delimiter ";" -Force -Encoding UTF8 -NoTypeInformation  
