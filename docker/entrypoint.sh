#!/bin/sh
set -e

if [ -n "$NETEM" ]; then
    tc qdisc replace dev eth0 root netem $NETEM
    echo "netem on eth0: $NETEM"
fi

exec "$@"
