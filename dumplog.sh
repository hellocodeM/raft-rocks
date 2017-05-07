#!/bin/bash

for addr in http://localhost:{8000..8002}/raft/log; do
    echo "======================= log at $addr ========================="
    curl $addr
done
