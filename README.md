# some-kvstore
A sharded fault-tolerent persistent K/V store that's hopefully fast.

This project is inspired by the MIT Distributed Systems class, but doesn't utilize any of their codebase.

references:
- [UW DS labs page](https://gitlab.cs.washington.edu/dwoos/452-labs/-/wikis/home)
- [MIT DS labs page](http://nil.csail.mit.edu/6.824/2015/index.html)

Basically the sharded key/value store consists of a shard master and a bunch of replication groups, each of which is a Raft cluster.

## RPC Services

within Raft cluster
- node join
- start cluster
- request vote
- append entry

replication group
- Get <- fe
- Put <- fe
- Append <- fe
- Delete <- fe
- get shard <- rg

shard master
- client registration <- fe, rg
- Get Configuration <- fe, rg
- join <- fe
- leave <- fe

## Shard Master

APIs

- Join
- Leave
- Move
- Query

## Replication Group in Raft

A replication groups manages one or more logical shards of the whole dataset, utilizing Raft protocol to reach consensus within a group.

Supporting Operations
- Get

    returns nil when key doesn't exist, not throwing error

- Put

    stores a key-value pair, overwrite old value on putting with existing key

- Append

    append passed in value to the end of existing value, equivalent to Put if key does not previously exist

- Delete

    delete key from store, pass silently if key not exist, not throwing error

### Server Side

#### Raft Cluster
Consensus protocol. Each node in the cluster manages a KVStore state machine to replicate the key/value pairs stored by client.

#### KVStore State Machine
Available commands for clent
- get
- put
- append
- delete

Commands for other replication group
- getShard

    callee returns and deletes local shard (or postpone delete maybe??)

- ownShard

    callee takes ownership of the passed in shard

### Client
- put
- get
- append
- delete

## Persistence

## TODO
- [ ] raft
    - [x] import old raft implementation
    - [ ] optimization
        - [ ] membership change
        - [ ] log compaction
    - [ ] client request idempotency cache and cleaning up
        - [ ] implementation
            - [ ] add finished seq list in rpc.ClientRequest and in LogEntry
            - [ ] remove finished request when processing LogEntry
            - [ ] client keeps track of finished request and piggyback in following requests
        - [ ] test
- [x] redesign and refactor RPC calls flow
- [x] single replication group
    - [x] KV store as state machine
        - [x] implementation
        - [x] test
    - [x] integration
        - [x] replication group node
        - [x] refactor existing raft implementation
            - [x] fix and refactor old tests
        - [x] client
        - [x] integration tests
- [ ] shard master
    - [ ] configuration
    - [ ] API
- [ ] integration
    - [ ] API and communication between replication groups and shard master
    - [ ] shard transfer between replication groups
- [ ] persistence
- [ ] deployment ready and demo