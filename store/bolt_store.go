package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
	"go.etcd.io/bbolt"
)

var (
	raftBucket = []byte("raft")
	stateKey   = []byte("persistent_state")
)

// BoltRaftStore persists Raft state in a local bbolt database.
type BoltRaftStore struct {
	db *bbolt.DB
}

func NewBoltRaftStore(path string, timeout time.Duration) (*BoltRaftStore, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: timeout})
	if err != nil {
		return nil, err
	}

	store := &BoltRaftStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *BoltRaftStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *BoltRaftStore) Save(state raft.PersistentState) error {
	if s == nil || s.db == nil {
		return errors.New("bolt raft store is closed")
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(raftBucket)
		if err != nil {
			return err
		}
		return bucket.Put(stateKey, data)
	})
}

func (s *BoltRaftStore) Load() (raft.PersistentState, error) {
	if s == nil || s.db == nil {
		return raft.PersistentState{}, errors.New("bolt raft store is closed")
	}

	var state raft.PersistentState
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(raftBucket)
		if bucket == nil {
			return nil
		}

		data := bucket.Get(stateKey)
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &state)
	})
	if err != nil {
		return raft.PersistentState{}, err
	}

	return state, nil
}

func (s *BoltRaftStore) init() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(raftBucket)
		return err
	})
}
