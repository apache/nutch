#!/bin/sh

function isRunning {
    id=$(docker ps -a | grep $1 | awk '{print $1}')
    echo $id
}
# cassandra
cassandraNodeName=CASS01
# elasticsearch
esNodeName=ES01
# apache nutch
nutchNodeName=NUTCH01