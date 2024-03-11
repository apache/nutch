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

FROM alpine:3.13
MAINTAINER Apache Nutch Committers <dev@nutch.apache.org>

WORKDIR /root/

# Install dependencies
RUN apk update
RUN apk --no-cache add apache-ant bash git openjdk11

RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> $HOME/.bashrc
env NUTCH_HOME='/root/nutch_source/runtime/local'

# Checkout and build the Nutch master branch (1.x)
RUN git clone https://github.com/apache/nutch.git nutch_source && \
     cd nutch_source && \
     ant runtime && \
     rm -rf build/ && \
     rm -rf /root/.ivy2/

# Create symlinks for runtime/local/bin/nutch and runtime/local/bin/crawl
RUN ln -sf $NUTCH_HOME/bin/nutch /usr/local/bin/
RUN ln -sf $NUTCH_HOME/bin/crawl /usr/local/bin/