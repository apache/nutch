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

We are currently using a shim (https://github.com/tballison/hadoop-safe-tika
because of binary conflicts in commons-io versions between what Hadoop supports and the more
modern features that Apache Tika and Apache POI were using in commons-io.

For now, all you have to do is update the fat jar dependencies:

1. tika-core-shaded in ivy/ivy.xml

2. tika-parsers-standard-package-shaded in src/plugin/parse-tika/ivy.xml

3. The library name version for tika-parsers-standard-package-shaded in src/plugin/parse-tika/plugin.xml

4. Repeat steps 2 and 3 for the language-identifier

5. Build Nutch and run all unit tests:

    $ cd ../../../
    $ ant clean runtime test

The following directions are what we used to do with thin jars. Hopefully, we'll
be able to get back to these directions once we have version harmony with Hadoop and Tika/POI.

1. Upgrade Tika dependency (tika-core) in ivy/ivy.xml

2. Upgrade Tika dependency in src/plugin/parse-tika/ivy.xml

3. Upgrade Tika's own dependencies in src/plugin/parse-tika/plugin.xml

   To get the list of dependencies and their versions execute:
    $ cd src/plugin/parse-tika/
    $ ant -f ./build-ivy.xml
    $ ls lib | sed 's/^/      <library name="/g' | sed 's/$/"\/>/g'

   In the plugin.xml replace all lines between
      <!-- dependencies of Tika (tika-parsers) -->
   and
      <!-- end of dependencies of Tika (tika-parsers) -->
   with the output of the command above.

4. (Optionally) remove overlapping dependencies between parse-tika and Nutch core dependencies:
   - check for libs present both in
       build/lib
     and
       build/plugins/parse-tika/
     (eventually with different versions)
   - duplicated libs can be added to the exclusions of transitive dependencies in
       build/plugins/parse-tika/ivy.xml
   - but the library versions in ivy/ivy.xml MUST correspond to those required by Tika

5. Remove the locally "installed" dependencies in src/plugin/parse-tika/lib/:

    $ rm -rf lib/

6. Repeat steps 2-5 for the language-identifier plugin which also depends on Tika modules

    $ cd ../language-identifier/

7. Build Nutch and run all unit tests:

    $ cd ../../../
    $ ant clean runtime test

