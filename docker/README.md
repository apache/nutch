# Nutch Dockerfile #

![Docker Pulls](https://img.shields.io/docker/pulls/apache/nutch?style=for-the-badge)
![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/apache/nutch?style=for-the-badge)
![Docker Image Version (latest semver)](https://img.shields.io/docker/v/apache/nutch?style=for-the-badge)
![Docker Stars](https://img.shields.io/docker/stars/apache/nutch?style=for-the-badge)
![Docker Automated build](https://img.shields.io/docker/automated/apache/nutch?style=for-the-badge)

Get up and running quickly with Nutch on Docker.

## What is Nutch?

![Nutch logo](https://cwiki.apache.org/confluence/download/attachments/115511997/nutch_logo_medium.gif "Nutch")

Apache Nutch is a highly extensible and scalable open source web crawler software project.

Nutch can run on a single machine, but gains a lot of its strength from running in a Hadoop cluster

## Docker Image

Current configuration of this image consists of components:

*	Nutch 1.x (branch "master")

##  Base Image

* [alpine:3.19](https://hub.docker.com/_/alpine/tags)

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

```bash
$(boot2docker shellinit | grep export) #may not be necessary
docker build -t apache/nutch .
```

## Usage

If not already running, start docker
```bash
boot2docker up
$(boot2docker shellinit | grep export)
```

Run a container interactively (`nutch` and `crawl` are on `PATH`; default command is `bash`):

```bash
docker run -t -i --name nutchcontainer apache/nutch
```

In another terminal, attach to a running container if needed:

```bash
docker exec -it nutchcontainer /bin/bash
```

Nutch is located in `$NUTCH_HOME` and is almost ready to run.
You will need to set seed URLs and update the `http.agent.name` configuration property in `$NUTCH_HOME/conf/nutch-site.xml` with your crawler's Agent Name.
For additional "getting started" information checkout the [Nutch Tutorial](https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial).

