# Nutch Dockerfile #

This directory contains a Dockerfile of [Nutch 2.X](http://nutch.apache.org) for [Docker](https://www.docker.com/).

## What is Nutch?

![Nutch logo](https://wiki.apache.org/nutch/FrontPage?action=AttachFile&do=get&target=nutch_logo_medium.gif "Nutch")

Apache Nutch is a highly extensible and scalable open source web crawler software project. 

Nutch can run on a single machine, but gains a lot of its strength from running in a Hadoop cluster

## Docker Image

Current configuration of this image consists of components:
	
*	Apache Hadoop 2.5.1
*	Apache HBase 0.98.8-hadoop2
*	Apache Nutch 2.X HEAD (this will ensure that you are always running off of bleeding edge)

##  Base Image

* [stackbrew/ubuntu:saucy](https://registry.hub.docker.com/u/stackbrew/ubuntu/)


## Installation

1. Install [Docker](https://www.docker.com/).

2a. Download automated build from public hub registry `docker pull nutch/nutch_with_hbase_hadoop`

2b. Build from files in this directory:

	$(boot2docker shellinit)
	docker build -t <new name for image> .

## Usage

Start docker
 
	boot2docker up
	$(boot2docker shellinit)

Start an image and enter shell. First command will start image and will print on stdout standard logs.

	IMAGE_PID=$(docker run -i -t  nutch_with_hbase_hadoop)
	docker exec -i -t $IMAGE_PID bash


Nutch is located in /opt/nutch/ and is almost ready to run.
Review configuration in /opt/nutch/conf/ and you can start crawling.

	echo 'http://nutch.apache.org' > seed.txt
	/opt/nutch/bin/nutch inject seed.txt
	/opt/nutch/bin/nutch generate -topN 10 -- this will return batchId
	/opt/nutch/bin/nutch fetch <batchId>
	/opt/nutch/bin/nutch parse <batchId>
	/opt/nutch/bin/nutch updatedb <batchId>
	[...]

## Resources

For more information on Nutch 2.X please see the [tutorials](http://wiki.apache.org/nutch/#Nutch_2.X_tutorial.28s.29) and [Nutch 2.X wiki space](http://wiki.apache.org/nutch/#Nutch_2.x).
