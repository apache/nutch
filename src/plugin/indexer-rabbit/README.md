indexer-rabbit plugin for Nutch
===============================

**indexer-rabbit plugin** is used for sending documents from one or more segments to a RabbitMQ server. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.rabbit.RabbitIndexWriter">
  <mapping>
    ...
  </mapping>
  <parameters>
    ...
  </parameters>
</writer>
```

Each `<writer>` element has two mandatory attributes:

* `<writer_id>` is a unique identification for each configuration. This feature allows Nutch to distinguish each configuration, even when they are for the same index writer. In addition, it allows to have multiple instances for the same index writer, but with different configurations.

* `org.apache.nutch.indexwriter.rabbit.RabbitIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-rabbit plugin**.

## Mapping

The mapping section is explained [here](https://wiki.apache.org/nutch/IndexWriters#Mapping_section). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
server.uri | URI with connection parameters in the form `amqp://<username>:<password>@<hostname>:<port>/<virtualHost>`<br>Where:<ul><li>`<username>` is the username for RabbitMQ server.</li><li>`<password>` is the password for RabbitMQ server.</li><li>`<hostname>` is where the RabbitMQ server is running.</li><li>`<port>` is where the RabbitMQ server is listening.</li><li>`<virtualHost>` is where the exchange is and the user has access.</li></ul> | amqp://guest:guest@localhost:5672/
binding | Whether the relationship between an exchange and a queue is created automatically.<br>**NOTE:** Binding between exchanges is not supported. | false
binding.arguments | Arguments used in binding. It must have the form `key1=value1,key2=value2`. This value is only used when the exchange's type is headers and the value of binding property is **true**. In other cases is ignored. | 
exchange.name | Name for the exchange where the messages will be sent. | 
exchange.options | Options used when the exchange is created. Only used when the value of `binding` property is **true**. It must have the form `type=<type>,durable=<durable>`<br>Where:<ul><li>`<type>` is **direct**, **topic**, **headers** or **fanout**</li><li>`<durable>` is **true** or **false** | type=direct,durable=true</li></ul>
queue.name | Name of the queue used to create the binding. Only used when the value of `binding` property is **true**. | nutch.queue
queue.options |  Options used when the queue is created. Only used when the value of `binding` property is **true**. It must have the form `durable=<durable>,exclusive=<exclusive>,auto-delete=<auto-delete>,arguments=<arguments>`<br>Where:<ul><li>`<durable>` is **true** or **false**</li><li>`<exclusive>` is **true** or **false**</li><li>`<auto-delete>` is **true** or **false**</li><li>`<arguments>` must be the form `key1:value1;key2:value2` | durable=true,exclusive=false,auto-delete=false</li></ul>
routingkey | The routing key used to route messages in the exchange. It only makes sense when the exchange type is **topic** or **direct**. | Value of `queue.name` property
commit.mode | **single** if a message contains only one document. In this case, a header with the action (write, update or delete) will be added. **multiple** if a message contains all documents. | multiple
commit.size | Amount of documents to send into each message if the value of `commit.mode` property is **multiple**. In **single** mode this value represents the amount of messages to be sent. | 250
headers.static | Headers to add to each message. It must have the form `key1=value1,key2=value2`. | 
headers.dynamic | Document's fields to add as headers to each message. It must have the form `field1,field2`. Only used when the value of `commit.mode` property is **single**. | 