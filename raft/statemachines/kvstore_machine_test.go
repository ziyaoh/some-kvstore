package statemachines

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ziyaoh/some-kvstore/raft/util"
)

func TestKVStoreMachineInitialize(t *testing.T) {
	t.Run("basic test", func(t *testing.T) {
		boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		defer os.Remove(boltPath)

		store, err := NewKVStoreMachine(boltPath)
		if err != nil {
			t.Error(err)
		}
		if store == nil {
			t.Fail()
		}
		store.Close()
	})
	t.Run("duplicate open", func(t *testing.T) {
		boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		defer os.Remove(boltPath)

		store1, err := NewKVStoreMachine(boltPath)
		if err != nil {
			t.Error(err)
		}
		if store1 == nil {
			t.Fail()
		}
		defer store1.Close()

		store2, err := NewKVStoreMachine(boltPath)
		if err == nil || store2 != nil {
			t.Error("KVStoreMachine duplicate open should have failed but succeeded")
		}
	})
	t.Run("reopen after close", func(t *testing.T) {
		boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		defer os.Remove(boltPath)

		store, err := NewKVStoreMachine(boltPath)
		if err != nil {
			t.Error(err)
		}
		if store == nil {
			t.Fail()
		}
		store.Close()

		store, err = NewKVStoreMachine(boltPath)
		if err != nil {
			t.Error(err)
		}
		if store == nil {
			t.Fail()
		}
		store.Close()
	})
	t.Run("multiple open", func(t *testing.T) {
		boltPath1 := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		defer os.Remove(boltPath1)

		store1, err := NewKVStoreMachine(boltPath1)
		if err != nil {
			t.Error(err)
		}
		if store1 == nil {
			t.Fail()
		}
		defer store1.Close()

		boltPath2 := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
		defer os.Remove(boltPath2)

		store2, err := NewKVStoreMachine(boltPath2)
		if err != nil {
			t.Error(err)
		}
		if store2 == nil {
			t.Fail()
		}
		defer store2.Close()
	})
}

func TestKVStoreMachineGetState(t *testing.T) {
	boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
	defer os.Remove(boltPath)
	t.Run("get state on empty store", func(t *testing.T) {
		store, err := NewKVStoreMachine(boltPath)
		if err != nil || store == nil {
			t.Fail()
		}
		defer store.Close()
		state, ok := store.GetState().(map[string]string)
		if !ok {
			t.Fail()
		}
		if len(state) != 0 {
			t.Fail()
		}
	})

	t.Run("simple get state", func(t *testing.T) {
		store, err := NewKVStoreMachine(boltPath)
		if err != nil || store == nil {
			t.Fail()
		}
		defer store.Close()

		expectState := make(map[string]string)

		pair1 := KVPair{
			Key:   []byte("test key"),
			Value: []byte("test value"),
		}
		expectState["test key"] = "test value"
		pair1Bytes, err := util.EncodeMsgPack(pair1)
		if err != nil {
			t.Error(err)
		}
		store.ApplyCommand(KVStorePut, pair1Bytes)
		state, ok := store.GetState().(map[string]string)
		if !ok {
			t.Fail()
		}
		if !reflect.DeepEqual(state, expectState) {
			t.Errorf("Expected state to be %v, got %v\n", expectState, state)
		}

		pair2 := KVPair{
			Key:   []byte("another test key"),
			Value: []byte("another test value"),
		}
		expectState["another test key"] = "another test value"
		pair2Bytes, err := util.EncodeMsgPack(pair2)
		if err != nil {
			t.Error(err)
		}
		store.ApplyCommand(KVStorePut, pair2Bytes)
		state, ok = store.GetState().(map[string]string)
		if !ok {
			t.Fail()
		}
		if !reflect.DeepEqual(state, expectState) {
			t.Errorf("Expected state to be %v, got %v\n", expectState, state)
		}
	})
}

func TestKVStoreHandleUnknownCommand(t *testing.T) {
	boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
	defer os.Remove(boltPath)

	store, err := NewKVStoreMachine(boltPath)
	if err != nil || store == nil {
		t.Fail()
	}
	defer store.Close()

	pair := KVPair{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	pairBytes, err := util.EncodeMsgPack(pair)
	if err != nil {
		t.Error(err)
	}
	res, err := store.ApplyCommand(10, pairBytes)

	if err == nil {
		t.Error("expecting KVStore return error on unknown command")
	}
	if res != nil {
		t.Errorf("expecting result on unknown command to be nil but got %v", res)
	}
}

func TestKVStoreMachinePut(t *testing.T) {
	cases := []struct {
		name     string
		data     []KVPair
		expected map[string]string
	}{
		{
			name: "simple put",
			data: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
			},
			expected: map[string]string{
				"test key": "test value",
			},
		},
		{
			name: "put zero-length bytes",
			data: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte{},
				},
			},
			expected: map[string]string{
				"test key": "",
			},
		},
		{
			name: "duplicate put",
			data: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("new value"),
				},
			},
			expected: map[string]string{
				"test key": "new value",
			},
		},
		{
			name: "general put",
			data: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
				KVPair{
					Key:   []byte("key two"),
					Value: []byte("value two"),
				},
				KVPair{
					Key:   []byte("key three"),
					Value: []byte("value three"),
				},
				KVPair{
					Key:   []byte("key two"),
					Value: []byte("new value two"),
				},
			},
			expected: map[string]string{
				"key one":   "value one",
				"key two":   "new value two",
				"key three": "value three",
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			store, err := NewKVStoreMachine(boltPath)
			if err != nil || store == nil {
				t.Fail()
			}
			defer store.Close()

			for _, pair := range testCase.data {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				store.ApplyCommand(KVStorePut, pairBytes)
			}
			state, ok := store.GetState().(map[string]string)
			if !ok {
				t.Fail()
			}
			if !reflect.DeepEqual(state, testCase.expected) {
				t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
			}
		})
	}
}

func TestKVStoreMachineGet(t *testing.T) {
	cases := []struct {
		name     string
		data     []KVPair
		testKey  []byte
		expected []byte
	}{
		{
			name:     "get from empty store",
			data:     []KVPair{},
			testKey:  []byte("test key"),
			expected: nil,
		},
		{
			name: "get non-existing key",
			data: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
			},
			testKey:  []byte("test key"),
			expected: nil,
		},
		{
			name: "get empty value",
			data: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte{},
				},
			},
			testKey:  []byte("key one"),
			expected: []byte{},
		},
		{
			name: "simple get",
			data: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
			},
			testKey:  []byte("key one"),
			expected: []byte("value one"),
		},
		{
			name: "get after overwriting",
			data: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("new value one"),
				},
			},
			testKey:  []byte("key one"),
			expected: []byte("new value one"),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			store, err := NewKVStoreMachine(boltPath)
			if err != nil || store == nil {
				t.Fail()
			}
			defer store.Close()

			for _, pair := range testCase.data {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				store.ApplyCommand(KVStorePut, pairBytes)
			}

			data := KVPair{
				Key:   testCase.testKey,
				Value: nil,
			}
			dataBytes, err := util.EncodeMsgPack(data)
			if err != nil {
				t.Error(err)
			}
			value, err := store.ApplyCommand(KVStoreGet, dataBytes)
			if err != nil {
				t.Errorf("Get should not fail but got %v", err)
			}

			if testCase.expected == nil && value != nil {
				t.Errorf("Expect value to be nil, got %v\n", value)
			}
			if value == nil && testCase.expected != nil {
				t.Errorf("Expect value to be %v, got nil\n", testCase.expected)
			}
			if !bytes.Equal(value, testCase.expected) {
				t.Errorf("Expect value to be %v, got %v\n", testCase.expected, value)
			}
		})
	}
}

func TestKVStoreMachineAppend(t *testing.T) {
	cases := []struct {
		name     string
		toPut    []KVPair
		toAppend []KVPair
		expected map[string]string
	}{
		{
			name: "simple append",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
			},
			toAppend: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("appended"),
				},
			},
			expected: map[string]string{
				"test key": "test valueappended",
			},
		},
		{
			name:  "append on non-existing key",
			toPut: []KVPair{},
			toAppend: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("appended"),
				},
			},
			expected: map[string]string{
				"test key": "appended",
			},
		},
		{
			name: "append nothing",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
			},
			toAppend: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: nil,
				},
			},
			expected: map[string]string{
				"test key": "test value",
			},
		},
		{
			name: "normal append",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
				KVPair{
					Key:   []byte("key two"),
					Value: []byte("value two"),
				},
				KVPair{
					Key:   []byte("key three"),
					Value: []byte("value three"),
				},
			},
			toAppend: []KVPair{
				KVPair{
					Key:   []byte("key two"),
					Value: []byte("appended"),
				},
			},
			expected: map[string]string{
				"key one":   "value one",
				"key two":   "value twoappended",
				"key three": "value three",
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			store, err := NewKVStoreMachine(boltPath)
			if err != nil || store == nil {
				t.Fail()
			}
			defer store.Close()

			for _, pair := range testCase.toPut {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				store.ApplyCommand(KVStorePut, pairBytes)
			}
			for _, pair := range testCase.toAppend {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				_, err = store.ApplyCommand(KVStoreAppend, pairBytes)
				if err != nil {
					t.Errorf("Append should not fail but got error %v", err)
				}
			}
			state, ok := store.GetState().(map[string]string)
			if !ok {
				t.Fail()
			}
			if !reflect.DeepEqual(state, testCase.expected) {
				t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
			}
		})
	}
}

func TestKVStoreMachineDelete(t *testing.T) {
	cases := []struct {
		name     string
		toPut    []KVPair
		toDelete []KVPair
		expected map[string]string
	}{
		{
			name: "simple delete",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
			},
			toDelete: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: nil,
				},
			},
			expected: map[string]string{},
		},
		{
			name: "delete non-existing key",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("test key"),
					Value: []byte("test value"),
				},
			},
			toDelete: []KVPair{
				KVPair{
					Key:   []byte("test key blah"),
					Value: nil,
				},
			},
			expected: map[string]string{
				"test key": "test value",
			},
		},
		{
			name: "normal delete",
			toPut: []KVPair{
				KVPair{
					Key:   []byte("key one"),
					Value: []byte("value one"),
				},
				KVPair{
					Key:   []byte("key two"),
					Value: []byte("value two"),
				},
				KVPair{
					Key:   []byte("key three"),
					Value: []byte("value three"),
				},
			},
			toDelete: []KVPair{
				KVPair{
					Key:   []byte("key two"),
					Value: nil,
				},
			},
			expected: map[string]string{
				"key one":   "value one",
				"key three": "value three",
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			boltPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvstore_%d", rand.Int()))
			defer os.Remove(boltPath)

			store, err := NewKVStoreMachine(boltPath)
			if err != nil || store == nil {
				t.Fail()
			}
			defer store.Close()

			for _, pair := range testCase.toPut {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				store.ApplyCommand(KVStorePut, pairBytes)
			}
			for _, pair := range testCase.toDelete {
				pairBytes, err := util.EncodeMsgPack(pair)
				if err != nil {
					t.Error(err)
				}
				_, err = store.ApplyCommand(KVStoreDelete, pairBytes)
				if err != nil {
					t.Errorf("Delete should not fail but got error %v", err)
				}
			}
			state, ok := store.GetState().(map[string]string)
			if !ok {
				t.Fail()
			}
			if !reflect.DeepEqual(state, testCase.expected) {
				t.Errorf("Expected state to be %v, got %v\n", testCase.expected, state)
			}
		})
	}
}
