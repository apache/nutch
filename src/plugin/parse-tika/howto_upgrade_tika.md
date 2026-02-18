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

# How to Upgrade Apache Tika in Nutch

Nutch uses official Apache Tika artifacts for document parsing and language detection.

## Upgrade Steps

1. Upgrade Tika dependency (tika-core) in ivy/ivy.xml

2. Upgrade Tika dependency (tika-parsers-standard-package) in src/plugin/parse-tika/ivy.xml
   Note: The tika-handler-boilerpipe module provides Boilerpipe support for text extraction
   (tika.extractor=boilerpipe configuration). This was moved from tika-core in TIKA-4138.

3. Upgrade Tika's own dependencies in src/plugin/parse-tika/plugin.xml

   To get the list of dependencies and their versions execute:
    $ cd src/plugin/parse-tika/
    $ ant -f ./build-ivy.xml
    $ ls lib | sed 's/^/      <library name="/g' | sed 's/$/"\/>/g'

   In the plugin.xml replace all lines between
      <!-- dependencies of Tika (tika-parsers-standard-package) -->
   and
      <!-- end of dependencies of Tika (tika-parsers-standard-package) -->
   with the output of the command above.

4. (Optionally) remove overlapping dependencies between parse-tika and Nutch core dependencies:
   - check for libs present both in
       build/lib
     and
       build/plugins/parse-tika/
     (eventually with different versions)
   - duplicated libs can be added to the exclusions of transitive dependencies in
       src/plugin/parse-tika/ivy.xml
   - but the library versions in ivy/ivy.xml MUST correspond to those required by Tika

5. Remove the locally "installed" dependencies in src/plugin/parse-tika/lib/:

    $ rm -rf lib/

6. Repeat steps 2-5 for the language-identifier plugin which also depends on Tika modules

    $ cd ../language-identifier/

   Update tika-langdetect-optimaize in src/plugin/language-identifier/ivy.xml
   Then regenerate the library list in plugin.xml:
    $ ant -f ./build-ivy.xml
    $ ls lib | sed 's/^/        <library name="/g' | sed 's/$/"\/>/g'

7. Build Nutch and run all unit tests:

    $ cd ../../../
    $ ant clean runtime test

