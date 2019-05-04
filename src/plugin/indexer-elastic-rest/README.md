indexer-elastic-rest plugin for Nutch 
=====================================

**indexer-elastic-rest plugin** is used for sending documents from one or more segments to Elasticsearch, but using Jest to connect with the REST API provided by Elasticsearch. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.elasticrest.ElasticRestIndexWriter">
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

* `org.apache.nutch.indexwriter.elasticrest.ElasticRestIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-elastic-rest plugin**.

## Mapping

The mapping section is explained [here](https://wiki.apache.org/nutch/IndexWriters#Mapping_section). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
host | The hostname or a list of comma separated hostnames to send documents to using Elasticsearch Jest. Both host and port must be defined. |  
port | The port to connect to using Elasticsearch Jest. | 9200
index | Default index to send documents to. | nutch
max.bulk.docs | Maximum size of the bulk in number of documents. | 250
max.bulk.size | Maximum size of the bulk in bytes. | 2500500
user | Username for auth credentials (only used when https is enabled) | user
password | Password for auth credentials (only used when https is enabled) | password
type | Default type to send documents to. | doc
https | **true** to enable https, **false** to disable https. If you've disabled http access (by forcing https), be sure to set this to true, otherwise you might get "connection reset by peer". | false
trustallhostnames | **true** to trust elasticsearch server's certificate even if its listed domain name does not match the domain they are hosted or **false** to check if the elasticsearch server's certificate's listed domain is the same domain that it is hosted on, and if it doesn't, then fail to index (only used when https is enabled) | false
languages | A list of strings denoting the supported languages (e.g. `en, de, fr, it`). If this value is empty all documents will be sent to index property. If not empty the Rest client will distribute documents in different indices based on their `languages` property. Indices are named with the following schema: `index separator language` (e.g. `nutch_de`). Entries with an unsupported `languages` value will be added to index `index separator sink` (e.g. `nutch_others`). | 
separator | Is used only if `languages` property is defined to build the index name (i.e. `index separator lang`). | _
sink | Is used only if `languages` property is defined to build the index name where to store documents with unsupported languages (i.e. `index separator sink`). | others 