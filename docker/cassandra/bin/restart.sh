#!/bin/sh

B_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

source "$B_DIR/nodes.sh"
source "$B_DIR/stop.sh"
source "$B_DIR/start.sh"