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

1. Upgrade Solr dependency in src/plugin/indexer-solr/ivy.xml

2. Upgrade the Solr specific dependencies in src/plugin/indexer-solr/plugin.xml
   To get the list of dependencies and their versions execute:
    $ cd src/plugin/indexer-solr/
    $ ant -f ./build-ivy.xml
    $ ls lib | sed 's/^/    <library name="/g' | sed 's/$/"\/>/g'

   In the plugin.xml replace all lines between
      <!-- Solr dependencies -->
   and
      <!-- end of Solr dependencies -->
   with the output of the command above.

4. (Optionally) remove overlapping dependencies between indexer-solr and Nutch core dependencies:
   - check for libs present both in
       build/lib
     and
       build/plugins/indexer-solr/
     (eventually with different versions)
   - duplicated libs can be added to the exclusions of transitive dependencies in
       build/plugins/indexer-solr/ivy.xml
   - but it should be made sure that the library versions in ivy/ivy.xml correspend to
     those required by Tika

5. Remove the locally "installed" dependencies in src/plugin/indexer-solr/lib/:

    $ rm -rf lib/

6. Build Nutch and run all unit tests:

    $ cd ../../../
    $ ant clean runtime test