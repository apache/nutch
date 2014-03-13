# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Parse-metatags plugin

The parse-metatags plugin consists of a HTMLParserFilter which takes as 
parameter a list of metatag names with '*' as default value. The values 
are separated by ';'.
In order to extract the values of the metatags description and keywords, 
you must specify in nutch-site.xml

<property>
  <name>metatags.names</name>
  <value>description;keywords</value>
</property>

Prefixes the names with 'metatag.' in the parse-metadata. For instance to 
index description and keywords, you need to activate the plugin index-metadata 
and set the value of the parameter 'index.metadata' to 'metatag.description;metatag.keywords'.
  
This code has been developed by DigitalPebble Ltd and offered to the community by ANT.com




