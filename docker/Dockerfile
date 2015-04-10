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

FROM ubuntu:14.04
MAINTAINER Michael Joyce <joyce@apache.org>

WORKDIR /root/

# Get the package containing apt-add-repository installed for adding repositories
RUN apt-get update && apt-get install -y software-properties-common

# Add the repository that we'll pull java down from.
RUN add-apt-repository -y ppa:webupd8team/java && apt-get update && apt-get upgrade -y

# Get Oracle Java 1.7 installed
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && apt-get install -y oracle-java7-installer oracle-java7-set-default

# Install various dependencies
RUN apt-get install -y ant openssh-server vim telnet subversion rsync curl build-essential 

# Set up JAVA_HOME
RUN echo 'export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")' >> $HOME/.bashrc

# Checkout and build the nutch trunk
RUN svn checkout https://svn.apache.org/repos/asf/nutch/trunk/ nutch_source && cd nutch_source && ant

# Convenience symlink to Nutch runtime local
RUN ln -s nutch_source/runtime/local $HOME/nutch
