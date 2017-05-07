#!/bin/bash

pkg=github.com/HelloCodeMing/raft-rocks/server
bin=./raftrocks

go build -o $bin $pkg

for p in {10000..10002}; do 
    log_dir=logs/$p
    addr=localhost:$p
    debug=localhost:$(( p - 2000 ))
    storage_path=/tmp/raftrocks-storage-$p
    [ ! -d ${log_dir} ] && mkdir -p $log_dir
    $bin -rpc_address $addr -debug_address $debug -alsologtostderr -log_dir ${log_dir} -v=1 -storage_path $storage_path & 
done

wait
