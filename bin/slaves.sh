#!/bin/bash
# 
# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   NUTCH_SLAVES    File naming remote hosts.  Default is ~/.slaves
##

usage="Usage: slaves.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ "$NUTCH_SLAVES" = "" ]; then
  export NUTCH_SLAVES=$HOME/.slaves
fi

for slave in `cat $NUTCH_SLAVES`; do
 echo -n $slave:\ 
 ssh -o ConnectTimeout=1 $slave "$@"
done
