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

indexer-opensearch1x plugin for Nutch 
================================

**indexer-opensearch1x plugin** is used for sending documents from one or more segments to an OpenSearch server. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.opensearch1x.OpenSearch1xIndexWriter">
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

* `org.apache.nutch.indexwriter.opensearch1x.OpenSearch1x.IndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-opensearch1x plugin**.

## Mapping

The mapping section is explained [here](https://cwiki.apache.org/confluence/display/NUTCH/IndexWriters#IndexWriters-Mappingsection). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
host | Comma-separated list of hostnames to send documents to using [TransportClient](https://static.javadoc.io/org.opensearch/opensearch/1.3.8/org/opensearch/client/transport/TransportClient.html). Either host and port must be defined. | 
port | The port to connect to using [TransportClient](https://static.javadoc.io/org.opensearch/opensearch/1.3.8/org/opensearch/client/transport/TransportClient.html). | 9300
scheme | The scheme (http or https) to connect to OpenSearch server. | https
index | Default index to send documents to. | nutch
username | Username for auth credentials | admin
password | Password for auth credentials | admin
trust.store.path | Path to the trust store |
trust.store.password | Password for trust store |
trust.store.type | Type of trust store | JKS
key.store.path | Path to the key store |
key.store.password | Password for the key and the key store |
key.store.type | Type of key store | JKS
max.bulk.docs | Maximum size of the bulk in number of documents. | 250
max.bulk.size | Maximum size of the bulk in bytes. | 2500500
exponential.backoff.millis | Initial delay for the [BulkProcessor](https://static.javadoc.io/org.opensearch/opensearch/1.3.8/org/opensearch/action/bulk/BulkProcessor.html) exponential backoff policy. | 100
exponential.backoff.retries | Number of times the [BulkProcessor](https://static.javadoc.io/org.opensearch/opensearch/1.3.8/org/opensearch/action/bulk/BulkProcessor.html) exponential backoff policy should retry bulk operations. | 10
bulk.close.timeout | Number of seconds allowed for the [BulkProcessor](https://static.javadoc.io/org.opensearch/opensearch/1.3.8/org/opensearch/action/bulk/BulkProcessor.html) to complete its last operation. | 600

## Authentication and SSL/TLS

It is highly recommended that users use at least basic authentication (modify the `username` and `password`!!!) and that they set up at least the trust store (1-way TLS).
For a "getting started" level introduction to setting up a trust store, see: [Connecting java-high-level-rest-client](https://opensearch.org/blog/connecting-java-high-level-rest-client-with-opensearch-over-https/).
For a more in depth treatment, see: [Configuring TLS certificates](https://opensearch.org/docs/latest/security/configuration/tls/).

Users may opt for 2-way TLS and skip basic authentication (`username` and `password`).  
To do this, specify both the `trust.store.*` parameters and the `key.store.*` parameters.

If users do not specify at least 1-way TLS (trust-store), this indexer logs a warning that this is a bad idea(TM), and it will proceed by completely ignoring all the SSL security.

## Design
This index writer was built to be as close as possible to Nutch's existing indexer-elastic code. We
therefore chose to use the to-be-deprecated-in-3.x `opensearch-rest-high-level-client`.
We should plan to migrate to the `java client` for 2.x, whenever the BulkProcessor has been added.
See the discussion on [NUTCH-2920](https://issues.apache.org/jira/projects/NUTCH/issues/NUTCH-2920).