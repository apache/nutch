# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# example of domain configuration for CloudSearch

DOMAIN="$1"

if [ "$DOMAIN" = "" ]; then
    echo "Need to specify a domain name as argument"
    exit -1;
fi

aws cloudsearch create-domain --domain-name $DOMAIN

aws cloudsearch define-index-field --domain-name $DOMAIN --name boost --type double --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name content --type text --sort-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name digest --type literal --sort-enabled false --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name host --type literal --sort-enabled false --facet-enabled true
aws cloudsearch define-index-field --domain-name $DOMAIN --name id --type literal --sort-enabled false --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name segment --type literal --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name title --type text --sort-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name tstamp --type date --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name url --type literal --sort-enabled false --facet-enabled false


