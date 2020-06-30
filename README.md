# some-kvstore
A sharded fault-tolerent persistent K/V store that's hopefully fast.

This project is inspired by the MIT Distributed Systems class, but doesn't utilize any of their codebase.

references:
- [UW DS labs page](https://gitlab.cs.washington.edu/dwoos/452-labs/-/wikis/home)
- [MIT DS labs page](http://nil.csail.mit.edu/6.824/2015/index.html)

Basically the sharded key/value store consists of a shard orchestrator and a bunch of replication groups, each of which is a Raft cluster.

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

shard orchestrator
- client registration <- fe, rg
- Get Configuration <- fe, rg
- join <- fe
- leave <- fe

## Sharding

Shard Orchestrator decides mapping between replication group and shards.

Query API of the Shard Orchestrator returns a configuration according to version number (or maybe just the latest one). Query from a replication group should contains a list of current addrs within the group (for membership changing).

Join, Leave, Move should change the current configuration and increment the version number.

Client operation on a kv pair should start by querying the configuration then talk to the replication group.

Replication Groups periodically query the latest configuration and transfer shard with each other.

### Configuration

- version number
- map of GID to list of addr
- map of shard to GID

### Shard Transfer

All nodes in a replication group periodically queries the shard orchestrator for latest configuration. On configuration change, update local configuration, ask raft protocol to do necessary shards out. This is step is only effective on a leader node in the raft cluster. Other state should ignore this request.

A shard out stores a raft log. Once committed, this log entry requires

1. reads local kv within the shard
2. makes shard transfer rpc call to target replication group with the data
3. wait for success reply, then hide that shard in state machine, so future client request on that shard should fail

When receive a shard transfer rpc call

1. checks for idempotency, return if the request has been seen
2. stores a shard in log entry containing the data
3. on process, put all the kv pairs in kvstore, own the shard
4. return success

For shard in requests, replication group doesn't check against its local configuration and trust the caller making the right call. In case of discrepency (lagged local config or remote config), this should be ok after the next periodical query.

## Shard Orchestrator

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
    - [x] client request idempotency cache and cleaning up
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
            - [x] change to ack finished request seq
        - [x] integration tests
- [ ] shard orchestrator
    - [ ] configuration
    - [ ] API
    - [ ] client
- [ ] integration
    - [ ] API and communication between replication groups and shard orchestrator
    - [ ] shard transfer between replication groups
- [ ] persistence
- [ ] deployment ready and demo