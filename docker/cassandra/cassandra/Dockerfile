#
# Cassandra
# meabed/debian-jdk
# docker build -t meabed/cassandra:latest .
#
# sudo sysctl -w vm.max_map_count=2621444
# sudo su
# echo "vm.max_map_count=262144" >> /etc/sysctl.conf

# you might need to run this commands in DOCKER HOST
# ulimit -l unlimited
# ulimit -n 16240
# ulimit -c unlimited

FROM meabed/debian-jdk
MAINTAINER Mohamed Meabed "mo.meabed@gmail.com"

USER root
ENV DEBIAN_FRONTEND noninteractive


# ADD DataStax sources
RUN echo "deb http://debian.datastax.com/community stable main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
RUN curl -L http://debian.datastax.com/debian/repo_key | apt-key add -

RUN apt-get update

# Install cassandra
RUN apt-get install -y dsc21
RUN apt-get install -y opscenter

# Comment the ulimit setters by cassandra deamon
RUN sed  -i "/^[^#]*ulimit/ s/.*/#&/"  /etc/init.d/cassandra


RUN service cassandra stop
RUN rm -rf /var/lib/cassandra/data/system/*

RUN sed -ri 's/#PermitRootLogin yes/PermitRootLogin yes/g' /etc/ssh/sshd_config
RUN sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config
RUN sed -ri 's/#UsePAM no/UsePAM no/g' /etc/ssh/sshd_config


RUN sed -i "/^cluster_name:/ s|.*|cluster_name: 'Cassandra Cluster'\n|" /etc/cassandra/cassandra.yaml
RUN sed -i "/^rpc_address:/ s|.*|rpc_address: 0.0.0.0\n|" /etc/cassandra/cassandra.yaml

RUN sed -ri 's/\/var\/lib\/cassandra\/data/\/var\/lib\/cassandra\/shared\/data\/cassandra/g' /etc/cassandra/cassandra.yaml

# Run Cassandra as Root
RUN sed -ri 's/-c cassandra/-c root/g' /etc/init.d/cassandra

#RUN echo "broadcast_rpc_address: localhost" >>  /etc/cassandra/cassandra.yaml

VOLUME ["/data"]


#RUN rm -rf /var/lib/cassandra
RUN mkdir -p /var/lib/cassandra/shared/data
RUN ln -svf /data/cassandra/ /var/lib/cassandra/shared/data

RUN service ssh start && service opscenterd start && service cassandra start

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

VOLUME ["/data"]

CMD ["/etc/bootstrap.sh", "-d"]

# http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
EXPOSE 22 8888 7000 7001 7199 9160 9042 61620 61621
