package statemachines

import (
	"errors"
	"time"

	errHelp "github.com/pkg/errors"
	"github.com/ziyaoh/some-kvstore/raft/util"
	bolt "go.etcd.io/bbolt"
)

const (
	KVStoreGet uint64 = iota
	KVStorePut
	KVStoreAppend
	KVStoreDelete
)

const dataBucketKey = "data"

// KVPair is a key-value pair
type KVPair struct {
	Key   []byte
	Value []byte
}

// KVStoreMachine implements the raft.StateMachine interface.
// It manages one or more logical shards of the entire dataset.
type KVStoreMachine struct {
	store *bolt.DB
	path  string
}

// NewKVStoreMachine either creates a new store or reopens an existing store.
// Fails if another KVStoreMachine is currently open on the same path
func NewKVStoreMachine(path string) (*KVStoreMachine, error) {
	store, err := bolt.Open(path, 0666, &bolt.Options{
		Timeout: 2 * time.Second,
	})
	if err != nil {
		return nil, errHelp.Wrapf(err, "NewKVStoreMachine: bolt open on path %s fail\n", path)
	}

	store.Update(func(tx *bolt.Tx) error {
		getBucket(tx, []byte(dataBucketKey))
		return nil
	})

	return &KVStoreMachine{store: store, path: path}, nil
}

func getBucket(tx *bolt.Tx, bucket []byte) *bolt.Bucket {
	var err error
	b := tx.Bucket(bucket)
	if b == nil {
		b, err = tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			panic(err)
		}
	}
	return b
}

// Close closes the underlying boltDB
func (store *KVStoreMachine) Close() {
	err := store.store.Close()
	if err != nil {
		panic(err)
	}
}

// GetState returns the state of the state machine as an interface{}, which can
// be converted to the expected type using type assertions.
func (store *KVStoreMachine) GetState() interface{} {
	kvs := make(map[string]string)
	store.store.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, []byte(dataBucketKey))
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			kvs[string(key)] = string(value)
		}
		return nil
	})
	return kvs
}

// ApplyCommand applies the given state machine command to the KVStoreMachine, and
// returns a message and an error if there is one.
func (store *KVStoreMachine) ApplyCommand(command uint64, data []byte) ([]byte, error) {
	var kvPair KVPair
	err := util.DecodeMsgPack(data, &kvPair)
	if err != nil {
		return nil, err
	}

	switch command {
	case KVStoreGet:
		return store.handleGet(&kvPair)
	case KVStorePut:
		return store.handlePut(&kvPair)
	case KVStoreAppend:
		return store.handleAppend(&kvPair)
	case KVStoreDelete:
		return store.handleDelete(&kvPair)
	default:
		return nil, errors.New("unknown command type")
	}
}

func (store *KVStoreMachine) handleGet(kvPair *KVPair) ([]byte, error) {
	var result []byte = nil
	store.store.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, []byte(dataBucketKey))
		value := bucket.Get(kvPair.Key)
		if value == nil {
			return nil
		}
		result = make([]byte, len(value))
		copy(result, value)
		return nil
	})
	return result, nil
}

func (store *KVStoreMachine) handlePut(kvPair *KVPair) ([]byte, error) {
	store.store.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, []byte(dataBucketKey))
		err := bucket.Put(kvPair.Key, kvPair.Value)
		if err != nil {
			panic(err)
		}
		return nil
	})
	return nil, nil
}

func (store *KVStoreMachine) handleAppend(kvPair *KVPair) ([]byte, error) {
	store.store.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, []byte(dataBucketKey))
		value := bucket.Get(kvPair.Key)
		if value == nil {
			value = kvPair.Value
		} else {
			value = append(value, kvPair.Value...)
		}
		err := bucket.Put(kvPair.Key, value)
		if err != nil {
			panic(err)
		}
		return nil
	})
	return nil, nil
}

func (store *KVStoreMachine) handleDelete(kvPair *KVPair) ([]byte, error) {
	store.store.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, []byte(dataBucketKey))
		err := bucket.Delete(kvPair.Key)
		if err != nil {
			panic(err)
		}
		return nil
	})
	return nil, nil
}
