#!/bin/bash

pkg=github.com/HelloCodeMing/raft-rocks/server
bin=./raftrocks

go build -o $bin $pkg

for addr in localhost:{10000..10002}; do 
    $bin -address $addr -alsologtostderr &
done

wait