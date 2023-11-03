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

indexer-csv plugin for Nutch 
============================

**indexer-csv plugin** is used for writing documents to a CSV file. It does not work in distributed mode, the output is written to the local filesystem, not to HDFS, see [NUTCH-1541](https://issues.apache.org/jira/browse/NUTCH-1541). The configuration for the index writers is on **conf/index-writers.xml** file, included in the official Nutch distribution and it's as follow:

```xml
<writer id="<writer_id>" class="org.apache.nutch.indexwriter.csv.CSVIndexWriter">
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

* `org.apache.nutch.indexwriter.csv.CSVIndexWriter` corresponds to the canonical name of the class that implements the IndexWriter extension point. This value should not be modified for the **indexer-csv plugin**.

## Mapping

The mapping section is explained [here](https://cwiki.apache.org/confluence/display/NUTCH/IndexWriters#IndexWriters-Mappingsection). The structure of this section is general for all index writers.

## Parameters

Each parameter has the form `<param name="<name>" value="<value>"/>` and the parameters for this index writer are:

Parameter Name | Description | Default value
--|--|--
fields | Ordered list of fields (columns) in the CSV file | id,title,content
charset | Encoding of CSV file | UTF-8
separator | Separator between fields (columns) | ,
valuesep | Separator between multiple values of one field | \|
quotechar | Quote character used to quote fields containing separators or quotes | &quot;
escapechar | Escape character used to escape a quote character | &quot;
maxfieldlength | Max. length of a single field value in characters | 4096
maxfieldvalues | Max. number of values of one field, useful for, e.g., the anchor texts field | 12
header | Write CSV column headers | true
outpath | Output path / directory (local filesystem path, relative to current working directory) | csvindexwriter
