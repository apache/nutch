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

# NOTE TO DEVELOPERS: Make sure this file passes linting tests
# by running https://github.com/replicatedhq/dockerfilelint

# BUILD_MODE can be either
#  0 == Nutch master branch source install with 'crawl' and 'nutch' scripts on PATH
#  1 == Same as mode 0 with addition of Nutch REST Server
#  2 == Same as mode 1 with addition of Nutch WebApp
ARG BUILD_MODE=0

FROM alpine:3.13 AS base

ARG SERVER_PORT=8081
ARG SERVER_HOST=0.0.0.0
ARG WEBAPP_PORT=8080

LABEL maintainer="Apache Nutch Developers <dev@nutch.apache.org>"
LABEL org.opencontainers.image.authors="Apache Nutch Developers <dev@nutch.apache.org>"
LABEL org.opencontainers.image.description="Docker image for running Apache Nutch, a highly extensible and scalable open source web crawler software project. Visit the project website at https://nutch.apache.org"
LABEL org.opencontainers.image.documentation="https://hub.docker.com/r/apache/nutch"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://raw.githubusercontent.com/apache/nutch/master/docker/Dockerfile"
LABEL org.opencontainers.image.title="Apache Nutch 1.x Docker Image"
LABEL org.opencontainers.image.url="https://hub.docker.com/r/apache/nutch"
LABEL org.opencontainers.image.vendor="Apache Nutch https://nutch.apache.org"

WORKDIR /root/

# Install dependencies
RUN apk update
RUN apk --no-cache add apache-ant bash git openjdk11 supervisor

# Establish environment variables
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> $HOME/.bashrc
ENV JAVA_HOME='/usr/lib/jvm/java-11-openjdk'
ENV NUTCH_HOME='/root/nutch_source/runtime/local'

# Checkout and build the Nutch master branch (1.x)
RUN git clone https://github.com/apache/nutch.git nutch_source && \
     cd nutch_source && \
     ant runtime && \
     rm -rf build/ && \
     rm -rf /root/.ivy2/

# Create symlinks for runtime/local/bin/nutch and runtime/local/bin/crawl
RUN ln -sf $NUTCH_HOME/bin/nutch /usr/local/bin/
RUN ln -sf $NUTCH_HOME/bin/crawl /usr/local/bin/

FROM base AS branch-version-0

RUN echo "Nutch master branch source install with 'crawl' and 'nutch' scripts on PATH"

FROM base AS branch-version-1

RUN echo "Nutch master branch source install with 'crawl' and 'nutch' scripts on PATH and Nutch REST Server on $SERVER_HOST:$SERVER_PORT"
ARG SERVER_PORT=8081
ARG SERVER_HOST=0.0.0.0

ENV SERVER_PORT=$SERVER_PORT
ENV SERVER_HOST=$SERVER_HOST

# Arrange necessary setup for supervisord
RUN mkdir -p /var/log/supervisord
COPY ./config/supervisord_startserver.conf /etc/supervisord.conf

# Expose port for server which can only be accessed if 
# the same port is published when the container is run.
EXPOSE $SERVER_PORT

ENTRYPOINT [ "supervisord", "--nodaemon", "--configuration", "/etc/supervisord.conf" ]

FROM base AS branch-version-2

RUN echo "Nutch master branch source install with 'crawl' and 'nutch' scripts on PATH, Nutch REST Server on $SERVER_HOST:$SERVER_PORT and WebApp on this container port $WEBAPP_PORT"
ARG SERVER_PORT=8081
ARG SERVER_HOST=0.0.0.0
ARG WEBAPP_PORT=8080

ENV SERVER_PORT=$SERVER_PORT
ENV SERVER_HOST=$SERVER_HOST
ENV WEBAPP_PORT=$WEBAPP_PORT

# Install the webapp
RUN apk --no-cache add maven
RUN git clone https://github.com/apache/nutch-webapp.git nutch_webapp && \
     cd nutch_webapp && \
     mvn package

# Arrange necessary setup for supervisord
RUN mkdir -p /var/log/supervisord
COPY ./config/supervisord_startserver_webapp.conf /etc/supervisord.conf

# Expose ports for server and webapp, these can only be accessed if 
# the same ports are published when the container is run.
EXPOSE $SERVER_PORT
EXPOSE $WEBAPP_PORT

ENTRYPOINT [ "supervisord", "--nodaemon", "--configuration", "/etc/supervisord.conf" ]

FROM branch-version-$BUILD_MODE AS final
RUN echo "Successfully built image, see https://s.apache.org/m5933 for guidance on running a container instance."