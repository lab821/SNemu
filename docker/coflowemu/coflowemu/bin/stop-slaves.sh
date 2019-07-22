#!/usr/bin/env bash

# Starts the master on the machine this script is executed on.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin/coflowemu-config.sh"

"$bin"/coflowemu-daemons.sh stop coflowemu.framework.slave.Slave