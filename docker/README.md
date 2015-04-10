# Nutch Dockerfile #

Get up and running quickly with Nutch on Docker.

## What is Nutch?

![Nutch logo](https://wiki.apache.org/nutch/FrontPage?action=AttachFile&do=get&target=nutch_logo_medium.gif "Nutch")

Apache Nutch is a highly extensible and scalable open source web crawler software project.

Nutch can run on a single machine, but gains a lot of its strength from running in a Hadoop cluster

## Docker Image

Current configuration of this image consists of components:

*	Nutch 1.x

##  Base Image

* [ubuntu:14.04](https://registry.hub.docker.com/_/ubuntu/)

## Tips

You may need to alias docker to "docker --tls" if you see errors such as:

```
2015/04/07 09:19:56 Post http://192.168.59.103:2376/v1.14/containers/create?name=NutchContainer: malformed HTTP response "\x15\x03\x01\x00\x02\x02\x16"
```

The easiest way to do this:

1. ```alias docker="docker --tls"```

## Installation

1. Install [Docker](https://www.docker.com/).

2. Build from files in this directory:

	$(boot2docker shellinit | grep export)
	docker build -t apache/nutch .

## Usage

Start docker

	boot2docker up
	$(boot2docker shellinit | grep export)

Start up an image and attach to it

    docker run -t -i -d --name nutchcontainer apache/nutch /bin/bash
    docker attach --sig-proxy=false nutchcontainer

Nutch is located in ~/nutch and is almost ready to run.
You will need to set seed URLs and update the configuration with your crawler's Agent Name.
For additional "getting started" information checkout the [Nutch Tutorial](https://wiki.apache.org/nutch/NutchTutorial).
