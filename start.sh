#!/bin/bash

pkg=github.com/HelloCodeMing/raft-rocks/server
bin=./raftrocks

go build -o $bin $pkg

for addr in localhost:{10000..10002}; do 
    log_dir=logs/$addr
    [ ! -d ${log_dir} ] && mkdir -p $log_dir
    $bin -address $addr -alsologtostderr -log_dir ${log_dir} & 
done

wait
