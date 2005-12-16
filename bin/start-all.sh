#!/bin/bash

# Start all nutch daemons.  Run this on master node.

bin=`dirname $0`
bin=`cd $bin; pwd`

$bin/nutch-daemons.sh start datanode
$bin/nutch-daemon.sh start namenode
$bin/nutch-daemon.sh start jobtracker
$bin/nutch-daemons.sh start tasktracker
