# raft-rocks
A simple key/value store based on [raft][1] and [rocksdb][2]

# Motivation
It's my graduation project, about Raft consensus algorithm. 
Originally, I wrote these code for MIT 6.824 's [labraft][3], but it doesn't has real storage or rpc. 
So I completed it with grpc and RocksDB, which makes it a real High-Available Key/Value storage.

# Implementation
As through, the design and implementation is too Naive, there's tons of bugs in code. 

In MIT6.824, there's many unit-tests, the origin code base could PASS ALL those tests, but after migration to this project and some modification, 
I could make sure that these code could not pass tests in 6.824.

# Design
- Raft: follow the [Raft Consensus][1] paper, implements leader election, log replication, without snapshot or configuration change.
- KV: use RocksDB to store key/value data
- WAL: use RocksDB to store log entry, in `<index,command>` way
- IDL: protobuf
- RPC: grpc

[1]: https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf
[2]: https://github.com/facebook/rocksdb
[3]: https://github.com/HelloCodeMing/MIT6.824