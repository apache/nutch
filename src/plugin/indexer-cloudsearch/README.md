AWS CloudSearch plugin for Nutch 
================================

See [http://aws.amazon.com/cloudsearch/] for information on AWS CloudSearch.

Steps to use :

From runtime/local/bin

* Configure the AWS credentials 

Edit `~/.aws/credentials`, see [http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html] for details. Note that this should not be necessary when running Nutch on EC2.

* Edit ../conf/nutch-site.xml and check that 'plugin.includes' contains 'indexer-cloudsearch'. 

* (Optional) Test the indexing 

`./nutch indexchecker -D doIndex=true -D cloudsearch.batch.dump=true "http://nutch.apache.org/"`

if the agent name hasn't been configured in nutch-site.xml, it can be added on the command line with `-D http.agent.name=whateverValueDescribesYouBest`

you should see the fields extracted for the indexing coming up on the console.

Using the `cloudsearch.batch.dump` parameter allows to dump the batch to the local temp dir. The files has the prefix "CloudSearch_" e.g. `/tmp/CloudSearch_4822180575734804454.json`. This temp file can be used as a template when defining the fields in the domain creation (see below).

* Create a CloudSearch domain

This can be done using the web console [https://eu-west-1.console.aws.amazon.com/cloudsearch/home?region=eu-west-1#]. You can use the temp file generated above to bootstrap the field definition. 

You can also create the domain using the AWS CLI [http://docs.aws.amazon.com/cloudsearch/latest/developerguide/creating-domains.html] and the `createCSDomain.sh` example script provided. This script is merely as starting point which you should further improve and fine tune. 

Note that the creation of the domain can take some time. Once it is complete, note the document endpoint, or alternatively verify the region and domain name.

* Edit ../conf/nutch-site.xml and add `cloudsearch.endpoint` and `cloudsearch.region`. 

* Re-test the indexing

`./nutch indexchecker -D doIndex=true "http://nutch.apache.org/"`

and check in the CloudSearch console that the document has been succesfully indexed.

Additional parameters

* `cloudsearch.batch.maxSize` \: can be used to limit the size of the batches sent to CloudSearch to N documents. Note that the default limitations still apply.

* `cloudsearch.batch.dump` \: see above. Stores the JSON representation of the document batch in the local temp dir, useful for bootstrapping the index definition.

Note

The CloudSearchIndexWriter will log any errors while sending the batches to CloudSearch and will resume the process without breaking. This means that you might not get all the documents in the index. You should check the log files for errors. Using small batch sizes will limit the number of documents skipped in case of error.

Any fields not defined in the CloudSearch domain will be ignored by the CloudSearchIndexWriter. Again, the logs will contain a trace of any field names skipped.



  


