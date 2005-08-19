#!/bin/bash
# 
# Runs a Nutch command as a daemon.
#
# Environment Variables
#
#   NUTCH_LOG_DIR   Where log files are stored.  PWD by default.
#   NUTCH_MASTER    host:path where nutch code should be rsync'd from
##

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

# get log directory
if [ "$NUTCH_LOG_DIR" = "" ]; then
  NUTCH_LOG_DIR=$PWD
fi

# some variables
log=$NUTCH_LOG_DIR/nutch-$command-`hostname`.log
pid=/tmp/nutch-$USER-$command.pid

case $startStop in

  (start)

    if [ -f $pid ]; then
      if [ -a /proc/`cat $pid` ]; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    root=`dirname $this`/..
    if [ "$NUTCH_MASTER" != "" ]; then
      echo rsync from $NUTCH_MASTER
      rsync -a --delete --exclude=.svn $NUTCH_MASTER/ $root
    fi

    cd $root
    echo starting $command, logging to $log
    nohup bin/nutch $command "$@" >& $log < /dev/null &
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


