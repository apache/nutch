#
# Nutch
# meabed/debian-jdk
# docker build -t meabed/nutch:latest .
#

FROM meabed/debian-jdk
MAINTAINER Mohamed Meabed "mo.meabed@gmail.com"

USER root
ENV DEBIAN_FRONTEND noninteractive

ENV NUTCH_VERSION 2.3

#ant
RUN apt-get install -y ant

#Download nutch

RUN mkdir -p /opt/downloads && cd /opt/downloads && curl -SsfLO "http://archive.apache.org/dist/nutch/$NUTCH_VERSION/apache-nutch-$NUTCH_VERSION-src.tar.gz"
RUN cd /opt && tar xvfz /opt/downloads/apache-nutch-$NUTCH_VERSION-src.tar.gz
#WORKDIR /opt/apache-nutch-$NUTCH_VERSION
ENV NUTCH_ROOT /opt/apache-nutch-$NUTCH_VERSION
ENV HOME /root

#Nutch-default
# RUN sed -i '/^  <name>http.agent.name<\/name>$/{$!{N;s/^  <name>http.agent.name<\/name>\n  <value><\/value>$/  <name>http.agent.name<\/name>\n  <value>iData Bot<\/value>/;ty;P;D;:y}}' $NUTCH_ROOT/conf/nutch-default.xml

RUN vim -c 'g/name="gora-cassandra"/+1d' -c 'x' $NUTCH_ROOT/ivy/ivy.xml
RUN vim -c 'g/name="gora-cassandra"/-1d' -c 'x' $NUTCH_ROOT/ivy/ivy.xml

RUN cd $NUTCH_ROOT && ant runtime

#native libs
RUN rm  $NUTCH_ROOT/lib/native/*
#RUN mkdir -p $NUTCH_ROOT/lib/native/Linux-amd64-64
#RUN curl -Ls http://dl.bintray.com/meabed/hadoop-debian/hadoop-native-64-2.5.1.tar|tar -x -C $NUTCH_ROOT/lib/native/Linux-amd64-64/


#Modification and compilation again

ADD plugin/nutch2-index-html/src/plugin/ $NUTCH_ROOT/src/plugin/
RUN sed  -i '/dir="index-more" target="deploy".*/ s/.*/&\n     <ant dir="index-html" target="deploy"\/>/' $NUTCH_ROOT/src/plugin/build.xml
RUN sed  -i '/dir="index-more" target="clean".*/ s/.*/&\n     <ant dir="index-html" target="clean"\/>/' $NUTCH_ROOT/src/plugin/build.xml


RUN cd $NUTCH_ROOT && ant runtime

RUN ln -s /opt/apache-nutch-$NUTCH_VERSION/runtime/local /opt/nutch

ENV NUTCH_HOME /opt/nutch

# urls folder we will use in crawling $NUTCH_HOME/bin/crawl urls crawlId(test01) elasticsearch_node_name(iData) iteration(1)
RUN mkdir $NUTCH_HOME/urls
# Adding test urls to use in crawling
CMD mkdir -p $NUTCH_HOME/testUrls
ADD testUrls $NUTCH_HOME/testUrls

# Adding rawcontent that hold html of the page field in index to elasticsearch
RUN sed  -i '/field name="date" type.*/ s/.*/&\n\n        <field name="rawcontent" type="text" sstored="true" indexed="true" multiValued="false"\/>\n/' $NUTCH_HOME/conf/schema.xml

# remove nutche-site.xml default file to replace it by our configuration
RUN rm $NUTCH_HOME/conf/nutch-site.xml
ADD config/nutch-site.xml $NUTCH_HOME/conf/nutch-site.xml

# Port that nutchserver will use
ENV NUTCHSERVER_PORT 8899

#RUN cd $NUTCH_HOME && ls -al

#RUN mkdir -p /opt/nutch/urls && cd /opt/crawl

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

VOLUME ["/data"]

CMD ["/etc/bootstrap.sh", "-d"]

EXPOSE 8899
