#!/bin/bash
# 
# Runs a Nutch command as a daemon.
#
# Environment Variables
#
#   NUTCH_LOG_DIR   Where log files are stored.  PWD by default.
#

# resolve links - $0 may be a softlink
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

usage="Usage: nutch-daemon [start|stop] [nutch-command] [args...]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

# get arguments
startStop=$1
shift
command=$1
shift

# some directories
this_dir=`dirname "$this"`

if [ "$NUTCH_LOG_DIR" = "" ]; then
  NUTCH_LOG_DIR=.
fi

# some variables
log=$NUTCH_LOG_DIR/nutch-$command-`hostname`.log
pid=/tmp/nutch-$command.pid

case $startStop in

  (start)

    if [ -f $pid ]; then
      if [ -a /proc/`cat $pid` ]; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    echo starting $command, logging to $log
    nohup $this_dir/nutch $command "$@" >& $log < /dev/null &
    echo $! > $pid
    sleep 1; head $log
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if [ -a /proc/`cat $pid` ]; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


