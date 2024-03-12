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

1. Upgrade various driver versions dependency in src/plugin/lib-selenium/ivy.xml

2. Upgrade Selenium's own dependencies in src/plugin/lib-selenium/plugin.xml

   To get a list of dependencies and their versions execute:
    $ ant -f ./build-ivy.xml
    $ ls lib | sed 's/^/     <library name="/g' | sed 's/$/">\n       <export name="*"\/>\n     <\/library>/g'

   Note that all dependent libraries are exported for a "library" plugin ("lib-selenium").

   N.B. The above Regex + Sed commands may not work if you are using MacOSX's Sed. In this instance you can instal GNU Sed as follows

   $ brew install gnu-sed --with-default-names

   You can then restart your terminal and the Regex + Sed command should work just fine!
