indexer-solr plugin for Nutch 
=============================

**indexer-solr plugin** is used for sending documents from one or more segments to a Solr server. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.solr.SolrIndexWriter">
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

* `org.apache.nutch.indexwriter.solr.SolrIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-solr plugin**.

## Mapping

The mapping section is explained [here](https://wiki.apache.org/nutch/IndexWriters#Mapping_section). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
type | Specifies the [SolrClient](https://lucene.apache.org/solr/5_5_0/solr-solrj/org/apache/solr/client/solrj/SolrClient.html) implementation to use. This is a string value of one of the following **cloud** or **http**. The values represent [CloudSolrServer](https://lucene.apache.org/solr/5_5_0/solr-solrj/org/apache/solr/client/solrj/impl/CloudSolrServer.html) or [HttpSolrServer](https://lucene.apache.org/solr/5_5_0/solr-solrj/org/apache/solr/client/solrj/impl/HttpSolrServer.html) respectively. | http
url | Defines the fully qualified URL of Solr into which data should be indexed. Multiple URL can be provided using comma as a delimiter. When the value of type property is **cloud**, the URL should not include any collections or cores; just the root Solr path. | http://localhost:8983/solr/nutch
collection | The collection used in requests. Only used when the value of type property is **cloud**. |  
weight.field | Field's name where the weight of the documents will be written. If it is empty no field will be used. |  
commitSize | Defines the number of documents to send to Solr in a single update batch. Decrease when handling very large documents to prevent Nutch from running out of memory.<br>**Note**: It does not explicitly trigger a server side commit. | 1000 
auth | Whether to enable HTTP basic authentication for communicating with Solr. Use the `username` and `password` properties to configure your credentials. | false
username | The username of Solr server. | username
password | The password of Solr server. | password

## schema.xml

In the distribution of the indexer-solr plugin there is a schema.xml file available. Nutch does not use this file, but it is provided to Solr users as a reference/guide to facilitate the configuration of Solr.