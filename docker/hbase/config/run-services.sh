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

set -x

# append ssh public key to authorized_keys file
#echo $AUTHORIZED_SSH_PUBLIC_KEY >> /home/hduser/.ssh/authorized_keys

# format the namenode if it's not already done
su -l -c 'mkdir -p /home/hduser/data/hadoop/nn /home/hduser/data/hadoop/dn && /opt/hadoop/bin/hadoop namenode -format' hduser

# start ssh daemon
service ssh start

# start zookeeper used for HDFS
service zookeeper start

# clear hadoop logs
rm -fr /opt/hadoop/logs/*

# start HDFS
su -l -c '/opt/hadoop/sbin/start-dfs.sh' hduser

# start YARN
su -l -c '/opt/hadoop/sbin/start-yarn.sh' hduser

#start HBASE
su -l -c '/opt/hbase/bin/start-hbase.sh' hduser

#start HBASE thrift
su -l -c '/opt/hbase/bin/hbase thrift start > /opt/hbase/logs/thrift.log 2>&1 &' hduser

sleep 1

# tail log directory
tail -n 1000 -f /opt/hadoop/logs/*.log /opt/hbase/logs/*.log /opt/nutch/logs/*.log
