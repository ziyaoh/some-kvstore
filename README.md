# some-kvstore
A sharded fault-tolerent persistent K/V store that's hopefully fast.

This project is inspired by the MIT Distributed Systems class, but doesn't utilize any of their codebase.

references:
- [UW DS labs page](https://gitlab.cs.washington.edu/dwoos/452-labs/-/wikis/home)
- [MIT DS labs page](http://nil.csail.mit.edu/6.824/2015/index.html)

Basically the sharded key/value store consists of a shard master and a bunch of replication groups, each of which is a Raft cluster.

## Shard Master

## Replication Group in Raft

A replication groups manages one or more logical shards of the whole dataset, utilizing Raft protocol to reach consensus within a group.

Client facing API
- Get
- Put
- Append

## Persistence

## TODO
- [ ] raft
    - [x] import old raft implementation
    - [ ] refactor existing code
        - [ ] better error handling
- [ ] single replication group
    - [ ] KV store as state machine
        - [ ] implementation
        - [ ] test
    - [ ] integrate KV store and raft
    - [ ] integration tests
- [ ] shard master
    - [ ] configuration
    - [ ] API
- [ ] integration
    - [ ] API and communication between replication groups and shard master
    - [ ] shard transfer between replication groups
- [ ] additional features & optimization
    - [ ] persistence
    - [ ] raft
        - [ ] membership change
        - [ ] log compaction
- [ ] deployment ready and demo