#!/bin/bash
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

export PATH=$PATH:/usr/local/sbin/
export PATH=$PATH:/usr/sbin/
export PATH=$PATH:/sbin

# Save Enviroemnt Variables incase ot attach to the container with new tty
env | awk '{split($0,a,"\n"); print "export " a[1]}' > /etc/env_profile

cassandra_env="$CASSANDRA_NODE_NAME"_PORT_9160_TCP_ADDR
cassandra_ip=$(printenv $cassandra_env)

sed  -i "/^gora\.cassandrastore\.servers.*/ s/.*//"  $NUTCH_HOME/conf/gora.properties
echo gora.cassandrastore.servers=$cassandra_ip:9160 >> $NUTCH_HOME/conf/gora.properties
vim -c '%s/localhost/'$cassandra_ip'/' -c 'x' $NUTCH_HOME/conf/gora-cassandra-mapping.xml

nutchserver_port=$(printenv NUTCHSERVER_PORT)

$NUTCH_HOME/bin/nutch nutchserver $nutchserver_port

echo "export PATH=$PATH" >> /etc/env_profile

if [[ $1 == "-d" ]]; then
    while true; do
        sleep 1000;
    done
fi

if [[ $1 == "-bash" ]]; then
    /bin/bash
fi
