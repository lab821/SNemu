#!/usr/bin/env bash

# Start all coflowemu daemons.
# Starts the master on this node.
# Starts a slave on each node specified in conf/slaves

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Load the Varys configuration
. "$bin/coflowemu-config.sh"

# Start Master
"$bin"/start-master.sh

# Start Slaves
"$bin"/start-slaves.sh
