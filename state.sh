#!/bin/bash

for addr in localhost:{10000..10002}/raft; do
    curl $addr -s | jq -c
done
