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

FROM alpine:3.19

LABEL maintainer="Apache Nutch Developers <dev@nutch.apache.org>"
LABEL org.opencontainers.image.authors="Apache Nutch Developers <dev@nutch.apache.org>"
LABEL org.opencontainers.image.description="Docker image for running Apache Nutch, a highly extensible and scalable open source web crawler software project. Visit the project website at https://nutch.apache.org"
LABEL org.opencontainers.image.documentation="https://hub.docker.com/r/apache/nutch"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://raw.githubusercontent.com/apache/nutch/master/docker/Dockerfile"
LABEL org.opencontainers.image.title="Apache Nutch 1.x Docker Image"
LABEL org.opencontainers.image.url="https://hub.docker.com/r/apache/nutch"
LABEL org.opencontainers.image.vendor="Apache Nutch https://nutch.apache.org"

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk'
ENV NUTCH_HOME='/opt/nutch/runtime/local'

# Install dependencies, build Nutch, and drop privileges for runtime
RUN apk --no-cache add \
        apache-ant=1.10.14-r0 \
        bash=5.2.21-r0 \
        git=2.43.7-r0 \
        openjdk17=17.0.14_p7-r0 \
    && addgroup -S nutch \
    && adduser -S -D -H -G nutch -h /opt/nutch nutch \
    && git clone https://github.com/apache/nutch.git /opt/nutch \
    && ant -f /opt/nutch/build.xml runtime \
    && rm -rf /opt/nutch/build/ \
    && rm -rf /root/.ivy2/ \
    && ln -sf "${NUTCH_HOME}/bin/nutch" /usr/local/bin/ \
    && ln -sf "${NUTCH_HOME}/bin/crawl" /usr/local/bin/ \
    && chown -R nutch:nutch /opt/nutch \
    && echo "Successfully built image, see https://s.apache.org/m5933 for guidance on running a container instance."

WORKDIR /opt/nutch

USER nutch

CMD ["/bin/bash"]
