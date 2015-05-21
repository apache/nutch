#!/bin/sh

B_DIR="`pwd`/"
docker pull meabed/debian-jdk

#
docker build -t "meabed/nutch:2.3" $B_DIR/nutch/
docker build -t "meabed/cassandra" $B_DIR/cassandra/
