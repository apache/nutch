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

# meabed/debian-jdk
# docker build -t apache/nutch:2.x .
#

FROM meabed/debian-jdk
MAINTAINER Nutch Developers "dev@nutch.apache.org"

USER root
ENV DEBIAN_FRONTEND noninteractive

#ant
RUN apt-get update && apt-get install -y ant subversion --fix-missing

#Download nutch

RUN mkdir -p /opt/downloads && cd /opt/downloads && svn co http://svn.apache.org/repos/asf/nutch/branches/2.x apache-nutch-2.x
RUN cd /opt 
RUN ln -s /opt/downloads/apache-nutch-2.x /opt/apache-nutch-2.x 
ENV NUTCH_ROOT /opt/apache-nutch-2.x
ENV HOME /root

#Nutch-default
# RUN sed -i '/^  <name>http.agent.name<\/name>$/{$!{N;s/^  <name>http.agent.name<\/name>\n  <value><\/value>$/  <name>http.agent.name<\/name>\n  <value>Nutch 2.X Cassandra Docker<\/value>/;ty;P;D;:y}}' $NUTCH_ROOT/conf/nutch-default.xml

RUN vim -c 'g/name="gora-cassandra"/+1d' -c 'x' $NUTCH_ROOT/ivy/ivy.xml
RUN vim -c 'g/name="gora-cassandra"/-1d' -c 'x' $NUTCH_ROOT/ivy/ivy.xml

RUN cd $NUTCH_ROOT && ant runtime

#native libs
RUN rm  $NUTCH_ROOT/lib/native/*
#RUN mkdir -p $NUTCH_ROOT/lib/native/Linux-amd64-64
#RUN curl -Ls http://dl.bintray.com/meabed/hadoop-debian/hadoop-native-64-2.5.1.tar|tar -x -C $NUTCH_ROOT/lib/native/Linux-amd64-64/


#Modification and compilation again

#ADD plugin/nutch2-index-html/src/plugin/ $NUTCH_ROOT/src/plugin/
#RUN sed  -i '/dir="index-more" target="deploy".*/ s/.*/&\n     <ant dir="index-html" target="deploy"\/>/' #$NUTCH_ROOT/src/plugin/build.xml
#RUN sed  -i '/dir="index-more" target="clean".*/ s/.*/&\n     <ant dir="index-html" target="clean"\/>/' #$NUTCH_ROOT/src/plugin/build.xml
#RUN cd $NUTCH_ROOT && ant runtime

RUN ln -s /opt/apache-nutch-2.x/runtime/local /opt/nutch

ENV NUTCH_HOME /opt/nutch

# urls folder we will use in crawling $NUTCH_HOME/bin/crawl urls crawlId(test01) elasticsearch_node_name(iData) iteration(1)
RUN mkdir $NUTCH_HOME/urls
# Adding test urls to use in crawling
CMD mkdir -p $NUTCH_HOME/testUrls
ADD testUrls $NUTCH_HOME/testUrls

# Adding rawcontent that hold html of the page field in index to elasticsearch
#RUN sed  -i '/field name="date" type.*/ s/.*/&\n\n        <field name="rawcontent" type="text" sstored="true" indexed="true" multiValued="false"\/>\n/' $NUTCH_HOME/conf/schema.xml

# remove nutche-site.xml default file to replace it by our configuration
RUN rm $NUTCH_HOME/conf/nutch-site.xml
ADD config/nutch-site.xml $NUTCH_HOME/conf/nutch-site.xml

# Port that nutchserver will use
ENV NUTCHSERVER_PORT 8899

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

VOLUME ["/data"]

CMD ["/etc/bootstrap.sh", "-d"]

EXPOSE 8899
