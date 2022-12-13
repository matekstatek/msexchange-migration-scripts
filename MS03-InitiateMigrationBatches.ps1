[CmdletBinding()]
param(
    # file should be an output from M02-CreateMigrationBatches from ./logfiles/databases 
    # - there are databases where mailboxes should be moved to
    [Parameter(Mandatory = $true)]
    [string]$inputFileName,
    [Parameter(Mandatory = $false)]
    [switch]$ResumeMoveRequests = $false,
    [Parameter(Mandatory = $false)]
    [switch]$whatif = $true
)
#script to initiate mailboxes migration

function write-log {
    param(
        [parameter(mandatory=$true,position=0)]
              $message,
        [parameter(mandatory=$false,position=1)]
            [string][validateSet('error','info','info2','warning','ok')]$type,
        [parameter(mandatory=$false,position=2)]
            [switch]$silent,
        [Parameter(mandatory=$false,position=3)]
            [switch]$skipTimestamp
    )

    if($null -eq $message) {
        $message=''
    }
    $message = ($message | out-String).trim() 
    
    try {
        if(-not $skipTimestamp) {
            $message = "$(Get-Date -Format "hh:mm:ss>") "+$type.ToUpper()+": "+$message
        }
        Add-Content -Path $logFile -Value $message
        if(-not $silent) {
            switch($type) {
                'error' {
                    write-host -ForegroundColor Red $message
                }
                'info' {
                    Write-Host -ForegroundColor DarkGray $message
                }
                'info2' {
                    Write-Host -ForegroundColor Blue $message
                }
                'warning' {
                    Write-Host -ForegroundColor Yellow $message
                }
                'ok' {
                    Write-Host -ForegroundColor Green $message
                }
                default {
                    Write-Host $message 
                }
            }
        }
    } catch {
        Write-Error 'not able to write to log. suggest to cancel the script run.'
        $_
    }    
}

# enable circular logging during the migration
# file related stuff
$date=get-date -format "yyyyMMdd_HHmm"
$currentPath=((get-location).path)+"\"

$logFilesPath=$currentPath+"LogFiles\"

if (!(Test-Path $LogFilesPath)) { 
    New-Item -Path $currentPath -Name "LogFiles" -ItemType Directory 
}

$inputFilePath = $currentPath+$inputFilename

$logFileName = "$($date)_initiate_batch.log"
$logfile = $logFilesPath+$logFileName

# mail stuff
$SMTPServer = "smtp.server@domain.com"
$from       = "reports@domain.com"
$to         = @("admin1@domain.com", "admin2@domain.com", "admin3@domain.com")

#counters
$initiated = 0
$skipped   = 0
$errors    = 0

write-log -message "Starting script." -type info

$InputCSVFile = import-csv $inputFilePath -Delimiter ";" -Encoding UTF8

$mbxNo = $InputCSVFile | 
    measure | 
        select -ExpandProperty count

write-log -message "Migration will be perforemd for $($mbxNo) mailboxes" -type info

# for every mbx in csv
foreach ($entry in $InputCSVFile) 
{
    # check if the guid is in an other moverequest (it cannot be in two places in the time)
    $oldRequests = get-moverequest | 
        Where-Object {
            $_.guid -eq "$($entry.guid)"
        }

    if($oldRequests -ne $null)
    {
        write-log -message "Error in initiating migration for mailbox with guid '$($entry.guid)' ($($entry.displayname)) to $($entry.database). - mailbox is already in the other MoveRequest." -type error
        $skipped ++
        
        # skip iteration
        continue
    }

    # if it is not in any move request
    try 
    {
        # move mailbox to specipic database
        # baditemlimit was set to 2 to avoid 99% of issues
        New-MoveRequest -Identity "$($entry.guid)" -BatchName "$inputFileName" -SuspendWhenReadyToComplete -TargetDatabase "$($entry.database)" -BadItemLimit 2 -warningaction silentlycontinue -whatif:$whatif |
            Out-Null

        write-log -message "Properly initiated migration for mailbox with guid '$($entry.guid)' ($($entry.displayname)) to $($entry.database)." -type ok
        $initiated ++
    } 
    catch 
    {
        write-log -message "Error in initiating migration for mailbox with guid '$($entry.guid)' ($($entry.displayname)) to $($entry.database)." -type error
        $errors ++
    }
}

# start time - to send an email every hour
$start = Get-Date

write-log -message "----------------------------------------"      -type info
write-log -message "Number of errors:  $($errors)"                 -type error
write-log -message "Number of skipped: $($skipped)"                -type warning
write-log -message "----------------------------------------"      -type info
write-log -message "Checking if all requests are AutoSuspended..." -type info
write-log -message "A mail will be sent every hour."               -type info

# if whatif is false
if($false -eq $whatif)
{
    do
    {
        # check how many move requests are AutoSuspended
        $autosuspended = $((get-moverequest -BatchName "$inputFileName" | 
                                group status | 
                                    ? {$_.name -eq "AutoSuspended"}).count)
        
        # check how long has the loop been working
        $end = Get-date
        $timeTaken = (new-timespan -Start $start -End $end).TotalHours

        # if the loop have been working for an hour - send an email to admins
        if($timeTaken -gt 1)
        {
            write-log "Email sent. $autosuspended/$mbxNo ready in $inputFileName." -type info2

            # send-mailmessage here
            $to |
                % { Send-MailMessage -Body "$autosuspended/$mbxNo ready in $inputFileName." -From $from -SmtpServer $SMTPServer -Subject "Report" -To $_ }

            # count from the beginning
            $start = Get-Date
        }
    } 
    while($autosuspended -lt $mbxNo-$skipped-$errors); # while a number of AutoSuspended is equal to number of mailboxes intended to migrate
}
else
{
    # if whatif is set to true
    write-host -message "whatif is set to $true. There's no batch like $inputFileName" -type info
}

# send last mail about AutoSuspended
$to |
    % { Send-MailMessage -Body "$autosuspended/$mbxNo ready in $inputFileName." -From $from -SmtpServer $SMTPServer -Subject "Report" -To $_ }

write-log -message "--------------------------------------------------------------"            -type info2
write-log -message "Email sent. $autosuspended/$mbxNo ready in $inputFileName."                -type info2
write-log -message "Properly initiated migration for $($initiated) out of $($mbxNo) mailboxes" -type ok
write-log -message "Number of  migration initiation errors: $($errors)"                        -type error
write-log -message "Number of  skipped mailboxes: $($skipped)"                                 -type warning
write-log -message "Finishing script initiate-migrationBatch"                                  -type info
write-log -message "--------------------------------------------------------------"            -type info2

# resuming requests
if($true -eq $ResumeMoveRequests)
{
    write-log -message "Resuming MoveRequest..." -type info

    Get-MoveRequest -BatchName inputFileName | 
        Resume-MoveRequest
    
    # sending emails until there's no mailbox with status othen than "Completed"
    # the loop works similarly to the previous one
    do
    {
        $completed = $((get-moverequest -BatchName "$inputFileName" | 
                            group status | 
                                ? {$_.name -eq "Completed"}).count)
        
        $end = Get-date
        $timeTaken = (new-timespan -Start $start -End $end).TotalHours

        if($timeTaken -gt 1)
        {
            write-log "Email sent. $completed/$mbxNo ready in $inputFileName." -type info2

            # send-mailmessage here
            $to |
                % { Send-MailMessage -Body "$completed/$mbxNo ready in $inputFileName." -From $from -SmtpServer $SMTPServer -Subject "Report" -To $_ }

            $start = Get-Date
        }
    } 
    while($completed -lt $autosuspended);
}

$to |
    % { Send-MailMessage -Body "$completed/$mbxNo ready in $inputFileName." -From $from -SmtpServer $SMTPServer -Subject "Report" -To $_ }

write-log "Done" -type ok