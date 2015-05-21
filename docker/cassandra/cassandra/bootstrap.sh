#!/bin/bash

export PATH=$PATH:/usr/local/sbin/
export PATH=$PATH:/usr/sbin/
export PATH=$PATH:/sbin

service ssh start && service opscenterd start && service cassandra start

# Save Enviroemnt Variables incase ot attach to the container with new tty
env | awk '{split($0,a,"\n"); print "export " a[1]}' > /etc/env_profile
container_ip=$(hostname -I | cut -d' ' -f1)
#sed -i "/^rpc_address:/ s|.*|\n|" /etc/cassandra/cassandra.yaml
#echo "rpc_address: "$container_ip >>  /etc/cassandra/cassandra.yaml
echo "broadcast_rpc_address: "$container_ip >>  /etc/cassandra/cassandra.yaml

echo "export PATH=$PATH" >> /etc/env_profile

if [[ $1 == "-d" ]]; then
    while true; do
        sleep 1000;
    done
fi

if [[ $1 == "-bash" ]]; then
    /bin/bash
fi
