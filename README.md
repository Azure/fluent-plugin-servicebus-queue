# Output plugin for sending records to an Azure Service Bus Queue for [Fluentd](http://fluentd.org)

## Requirements

| fluent-plugin-record-modifier  | fluentd | ruby |
|--------------------------------|---------|------|
| >= 1.0.0 | >= v0.14.0 | >= 2.1 |
|  < 1.0.0 | >= v0.12.0 | >= 1.9 |

## Configuration

    <match **>
        @type azure_servicebus_queue
        namespace servicebusNamespace
        queueName queueName
        accessKeyName send
        accessKeyValueFile /etc/password/queuePassword
        timeToLive 60
        <buffer>
            @type memory
            flush_interval 1s
        </buffer>
    </match>

Will send records to Azure Service Bus Queue with namespace of servicebusNamespace with queue name of queueName. Will use Shared access policy named send and the Primary/Secondary key in a file located in /etc/password/queuePassword with a time to live of one minute.

## Trademarks 

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow Microsoft's Trademark & Brand Guidelines. Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
