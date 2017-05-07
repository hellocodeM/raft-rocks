#!/bin/bash

for addr in localhost:{8000..8002}/raft/meta; do
    curl $addr -s | jq -c
done
