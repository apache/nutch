<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

indexer-elastic plugin for Nutch 
================================

**indexer-elastic plugin** is used for sending documents from one or more segments to an Elasticsearch server. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.elastic.ElasticIndexWriter">
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

* `org.apache.nutch.indexwriter.elastic.ElasticIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-elastic plugin**.

## Mapping

The mapping section is explained [here](https://cwiki.apache.org/confluence/display/NUTCH/IndexWriters#IndexWriters-Mappingsection). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
host | Comma-separated list of hostnames to send documents to using [TransportClient](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/client/transport/TransportClient.html). Either host and port must be defined. | 
port | The port to connect to using [TransportClient](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/client/transport/TransportClient.html). | 9300
scheme | The scheme (http or https) to connect to elastic server. | http
index | Default index to send documents to. | nutch
username | Username for auth credentials | elastic
password | Password for auth credentials | ""
auth | Whether to enable HTTP basic authentication with elastic. Use `username` and `password` properties to configure your credentials. | false
max.bulk.docs | Maximum size of the bulk in number of documents. | 250
max.bulk.size | Maximum size of the bulk in bytes. | 2500500
exponential.backoff.millis | Initial delay for the [BulkProcessor](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/action/bulk/BulkProcessor.html) exponential backoff policy. | 100
exponential.backoff.retries | Number of times the [BulkProcessor](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/action/bulk/BulkProcessor.html) exponential backoff policy should retry bulk operations. | 10
bulk.close.timeout | Number of seconds allowed for the [BulkProcessor](https://static.javadoc.io/org.elasticsearch/elasticsearch/5.3.0/org/elasticsearch/action/bulk/BulkProcessor.html) to complete its last operation. | 600
