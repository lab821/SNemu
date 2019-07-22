#!/usr/bin/env bash

# Start all coflowemu daemons.
# Run this on the master nde

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Load the Varys configuration
. "$bin/coflowemu-config.sh"

# Stop the slaves, then the master
"$bin"/stop-slaves.sh
"$bin"/stop-master.sh
