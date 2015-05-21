#!/bin/sh

CONTAINER=$1
docker inspect --format '{{ .NetworkSettings.IPAddress }}' $CONTAINER
