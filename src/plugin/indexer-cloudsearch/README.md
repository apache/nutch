AWS CloudSearch plugin for Nutch 
================================

See [http://aws.amazon.com/cloudsearch/] for information on AWS CloudSearch.

**indexer-cloudsearch plugin** is used for sending documents from one or more segments to Amazon CloudSearch. The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.cloudsearch.CloudSearchIndexWriter">
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

* `org.apache.nutch.indexwriter.cloudsearch.CloudSearchIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-cloudsearch plugin**.

## Mapping

The mapping section is explained [here](https://wiki.apache.org/nutch/IndexWriters#Mapping_section). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
endpoint | Endpoint where service requests should be submitted. | 
region | Region name. | 
batch.dump | **true** to store the JSON representation of the documents to a local temp dir. The files has the prefix "CloudSearch_" e.g. `/tmp/CloudSearch_4822180575734804454.json`. This temp file can be used as a template when defining the fields in the domain creation. | false
batch.maxSize | Maximum number of documents to send as a batch to CloudSearch. | -1

## Create a CloudSearch domain

This can be done using the web console [https://eu-west-1.console.aws.amazon.com/cloudsearch/home?region=eu-west-1#]. You can use the temp file generated above to bootstrap the field definition. 

You can also create the domain using the AWS CLI [http://docs.aws.amazon.com/cloudsearch/latest/developerguide/creating-domains.html] and the `createCSDomain.sh` example script provided. This script is merely as starting point which you should further improve and fine tune. 

Note that the creation of the domain can take some time. Once it is complete, note the document endpoint, or alternatively verify the region and domain name.

> The CloudSearchIndexWriter will log any errors while sending the batches to CloudSearch and will resume the process without breaking. This means that you might not get all the documents in the index. You should check the log files for errors. Using small batch sizes will limit the number of documents skipped in case of error.

> Any fields not defined in the CloudSearch domain will be ignored by the CloudSearchIndexWriter. Again, the logs will contain a trace of any field names skipped.








