#!/bin/bash

# Stop all nutch daemons.  Run this on master node.

bin=`dirname $0`
bin=`cd $bin; pwd`

$bin/nutch-daemon.sh stop jobtracker
$bin/nutch-daemons.sh stop tasktracker
$bin/nutch-daemon.sh stop namenode
$bin/nutch-daemons.sh stop datanode
