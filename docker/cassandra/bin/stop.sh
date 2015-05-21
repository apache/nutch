#!/bin/sh

B_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "$B_DIR/nodes.sh"

cassandraRunning=$(isRunning $cassandraNodeName)
esRunning=$(isRunning $esNodeName)
nutchRunning=$(isRunning $nutchNodeName)

if [ -n "${cassandraRunning}" ]; then
    docker kill $cassandraNodeName && docker rm $cassandraNodeName
fi

if [ -n "${esRunning}" ]; then
    docker kill $esNodeName && docker rm $esNodeName
fi

if [ -n "${nutchRunning}" ]; then
    docker kill $nutchNodeName && docker rm $nutchNodeName
fi