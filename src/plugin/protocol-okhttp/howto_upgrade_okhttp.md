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

1. Upgrade OkHttp dependency in src/plugin/protocol-okhttp/ivy.xml

2. Upgrade OkHttp's own dependencies in src/plugin/protocol-okhttp/plugin.xml

   To get the list of dependencies and their versions execute in the Nutch root
   folder:
    $ ant clean runtime
    $ ls build/plugins/protocol-okhttp/ \
         | grep '\.jar$' \
         | grep -vF protocol-okhttp.jar \
         | sed 's/^/      <library name="/g' | sed 's/$/"\/>/g'

   In the plugin.xml replace all lines between
      <!-- dependencies of OkHttp -->
   and
      <!-- end of dependencies of OkHttp -->
   with the output of the command above.

3. Build Nutch and run all unit tests:

    $ ant clean runtime test

   At least, run the protocol-okhttp unit tests:

    $ ant test-plugin -Dplugin=protocol-okhttp

4. (optionally but recommended) Run a test crawl using protocol-okhttp