#!/bin/sh
#
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

B_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DOCKER_DATA_FOLDER=$B_DIR/docker-data

chmod -R 777 $DOCKER_DATA_FOLDER

source "$B_DIR/nodes.sh"
source "$B_DIR/stop.sh"

cassandraId=$(docker run -d -P -v $DOCKER_DATA_FOLDER:/data:rw --name $cassandraNodeName apache/cassandra)
cassandraIP=$("$B_DIR"/ipof.sh $cassandraId)

# -p 9200:9200
# http://dockerhost:9200/_plugin/kopf/
# http://dockerhost:9200/_plugin/HQ/

docker run -d -p 8899:8899 -P -e CASSANDRA_NODE_NAME=$cassandraNodeName -it --link $cassandraNodeName:$cassandraNodeName -v $DOCKER_DATA_FOLDER:/data:rw --name $nutchNodeName apache/nutch:2.x
# apache/nutch2cassandra
