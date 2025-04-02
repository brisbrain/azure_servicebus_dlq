<#
.SYNOPSIS
    Clears Dead Letter Queues from Azure Service Bus Topics and Queues.

.DESCRIPTION
    This script removes messages from Dead Letter Queues in Azure Service Bus Topics and Queues.
    Includes WhatIf support and configurable maximum message removal.

.PARAMETER Namespace
    The Azure Service Bus namespace name.

.PARAMETER ResourceGroup
    The resource group containing the Service Bus namespace.

.PARAMETER QueueName
    Optional: Specific queue name to process (if not specified, processes all queues).

.PARAMETER TopicSubscription
    Optional: Specific topic/subscription in format "topicname/subscriptionname" (if not specified, processes all).

.PARAMETER MaxMessages
    Maximum number of messages to remove per queue/topic (default: 1000).

.PARAMETER WhatIf
    Switch to simulate the operation without actually removing messages.

.EXAMPLE
    .\Clear-ServiceBusDLQ.ps1 -Namespace "my-namespace" -ResourceGroup "my-rg" -QueueName "myqueue" -WhatIf
    # Simulates clearing DLQ for specific queue

.EXAMPLE
    .\Clear-ServiceBusDLQ.ps1 -Namespace "my-namespace" -ResourceGroup "my-rg" -TopicSubscription "mytopic/mysub" -MaxMessages 500
    # Clears up to 500 messages from specific topic subscription DLQ
#>

[CmdletBinding(SupportsShouldProcess=$true)]
param (
    [Parameter(Mandatory=$true)]
    [string]$Namespace,

    [Parameter(Mandatory=$true)]
    [string]$ResourceGroup,

    [Parameter(Mandatory=$false)]
    [string]$QueueName,

    [Parameter(Mandatory=$false)]
    [string]$TopicSubscription,

    [Parameter(Mandatory=$false)]
    [int]$MaxMessages = 1000,

    [Parameter(Mandatory=$false)]
    [switch]$WhatIf
)

# Function to process dead letter messages remains the same as previous version
function Clear-DeadLetterQueue {
    param (
        [string]$EntityPath,
        [string]$EntityType
    )
    # ... (same implementation as previous version) ...
}

# Main script execution
try {
    if (-not (Get-Module -ListAvailable -Name Az.ServiceBus)) {
        Write-Error "Az.ServiceBus module not found. Please install it using: Install-Module -Name Az.ServiceBus"
        exit 1
    }

    if (-not (Get-AzContext)) {
        Connect-AzAccount
    }

    # Process specific queue if specified
    if ($QueueName) {
        Write-Host "Processing specified queue..." -ForegroundColor Cyan
        $queue = Get-AzServiceBusQueue -ResourceGroupName $ResourceGroup -NamespaceName $Namespace -Name $QueueName
        if ($queue -and $queue.DeadLetterMessageCount -gt 0) {
            Clear-DeadLetterQueue -EntityPath $queue.Name -EntityType "Queue"
        }
        elseif (-not $queue) {
            Write-Warning "Queue $QueueName not found"
        }
    }
    # Process specific topic/subscription if specified
    elseif ($TopicSubscription) {
        Write-Host "Processing specified topic/subscription..." -ForegroundColor Cyan
        $topicName = $TopicSubscription.Split('/')[0]
        $subName = $TopicSubscription.Split('/')[1]
        $sub = Get-AzServiceBusSubscription -ResourceGroupName $ResourceGroup `
            -NamespaceName $Namespace `
            -TopicName $topicName `
            -Name $subName
        if ($sub -and $sub.DeadLetterMessageCount -gt 0) {
            Clear-DeadLetterQueue -EntityPath "$topicName/subscriptions/$subName" -EntityType "Topic"
        }
        elseif (-not $sub) {
            Write-Warning "Topic/Subscription $TopicSubscription not found"
        }
    }
    # Process all queues and topics if no specific entity specified
    else {
        # Get all queues
        Write-Host "Retrieving queues..." -ForegroundColor Cyan
        $queues = Get-AzServiceBusQueue -ResourceGroupName $ResourceGroup -NamespaceName $Namespace
        foreach ($queue in $queues) {
            if ($queue.DeadLetterMessageCount -gt 0) {
                Clear-DeadLetterQueue -EntityPath $queue.Name -EntityType "Queue"
            }
        }

        # Get all topics and subscriptions
        Write-Host "Retrieving topics and subscriptions..." -ForegroundColor Cyan
        $topics = Get-AzServiceBusTopic -ResourceGroupName $ResourceGroup -NamespaceName $Namespace
        foreach ($topic in $topics) {
            $subscriptions = Get-AzServiceBusSubscription -ResourceGroupName $ResourceGroup `
                -NamespaceName $Namespace `
                -TopicName $topic.Name
            
            foreach ($sub in $subscriptions) {
                if ($sub.DeadLetterMessageCount -gt 0) {
                    $entityPath = "$($topic.Name)/subscriptions/$($sub.Name)"
                    Clear-DeadLetterQueue -EntityPath $entityPath -EntityType "Topic"
                }
            }
        }
    }

    Write-Host "Dead Letter Queue cleanup completed" -ForegroundColor Green
}
catch {
    Write-Error "Script execution failed: $_"
    exit 1
}
