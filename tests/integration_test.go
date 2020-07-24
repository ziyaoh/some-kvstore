package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ziyaoh/some-kvstore/client"
	"github.com/ziyaoh/some-kvstore/raft/raft"
	"github.com/ziyaoh/some-kvstore/replicationgroup"
	"github.com/ziyaoh/some-kvstore/shardorchestrator"
	"github.com/ziyaoh/some-kvstore/util"
)

func TestSomeKVStoreIntegration(t *testing.T) {
	util.SuppressLoggers()

	orchNodes, err := shardorchestrator.CreateLocalShardOrchestrator(util.NumShards, raft.DefaultConfig())
	assert.Nil(t, err)
	defer shardorchestrator.CleanupShardOrchestrator(orchNodes)
	time.Sleep(500 * time.Millisecond)
	orchAddr := orchNodes[0].Self.Addr

	groupID1 := uint64(1)
	groupID2 := uint64(2)
	group1, err := replicationgroup.CreateEmptyLocalReplicationGroup(groupID1, raft.DefaultConfig(), orchAddr)
	assert.Nil(t, err)
	defer replicationgroup.CleanupReplicationGroup(group1)
	group2, err := replicationgroup.CreateEmptyLocalReplicationGroup(groupID2, raft.DefaultConfig(), orchAddr)
	assert.Nil(t, err)
	defer replicationgroup.CleanupReplicationGroup(group2)
	time.Sleep(500 * time.Millisecond)
	group1Addrs := []string{}
	for _, node := range group1 {
		group1Addrs = append(group1Addrs, node.Self.Addr)
	}
	group2Addrs := []string{}
	for _, node := range group2 {
		group2Addrs = append(group2Addrs, node.Self.Addr)
	}

	admin, err := shardorchestrator.AdminConnect(orchAddr)
	assert.Nil(t, err)
	client, err := client.NewClient(orchAddr)
	assert.Nil(t, err)

	t.Run("client operation fail on empty kv store", func(t *testing.T) {
		err := client.Put([]byte("key"), []byte("value"))
		assert.NotNil(t, err)
	})

	t.Run("two groups join", func(t *testing.T) {
		err := admin.Join(groupID1, group1Addrs)
		assert.Nil(t, err)
		err = admin.Join(groupID2, group2Addrs)
		assert.Nil(t, err)
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("normal client put and get", func(t *testing.T) {
		err := client.Put([]byte("key"), []byte("value"))
		assert.Nil(t, err)
		value, err := client.Get([]byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("value"), value)

		err = client.Append([]byte("key"), []byte("tail"))
		assert.Nil(t, err)
		value, err = client.Get([]byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("valuetail"), value)
	})

	t.Run("test shard transfer in case group leaves", func(t *testing.T) {
		err := admin.Leave(groupID1)
		assert.Nil(t, err)
		time.Sleep(300 * time.Millisecond)
		value, err := client.Get([]byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("valuetail"), value)

		err = admin.Join(groupID1, group1Addrs)
		assert.Nil(t, err)
		err = admin.Leave(groupID2)
		assert.Nil(t, err)
		time.Sleep(300 * time.Millisecond)

		value, err = client.Get([]byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("valuetail"), value)
	})

	t.Run("test all replica groups leave", func(t *testing.T) {
		err := admin.Leave(groupID1)
		assert.Nil(t, err)

		_, err = client.Get([]byte("key"))
		assert.NotNil(t, err)
	})
}
