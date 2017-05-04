#!/bin/bash

pkg=github.com/HelloCodeMing/raft-rocks/server
bin=./raftrocks

go build -o $bin $pkg

for p in {10000..10002}; do 
    log_dir=logs/$addr
    addr=localhost:$p
    storage_path=/tmp/raftrocks-storage-$p
    [ ! -d ${log_dir} ] && mkdir -p $log_dir
    $bin -address $addr -alsologtostderr -log_dir ${log_dir} -use_rocksdb -storage_path $storage_path & 
done

wait
