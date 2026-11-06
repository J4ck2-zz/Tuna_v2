package store

import (
	"bytes"
	"errors"

	"github.com/nutsdb/nutsdb"
)

type NutsDB struct {
	db *nutsdb.DB
}

const DefaultBucket = "DefaultBucket"

func OpenNutsDB(dir string) (*NutsDB, error) {
	db, err := nutsdb.Open(nutsdb.DefaultOptions, nutsdb.WithDir(dir))
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *nutsdb.Tx) error {
		if tx.ExistBucket(nutsdb.DataStructureBTree, DefaultBucket) {
			return nil
		}

		return tx.NewBucket(nutsdb.DataStructureBTree, DefaultBucket)
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &NutsDB{
		db: db,
	}, nil
}

func NewDefaultNutsDB(dir string) *NutsDB {
	db, err := OpenNutsDB(dir)
	if err != nil {
		panic(err)
	}

	return db
}

func (n *NutsDB) Get(key []byte) ([]byte, error) {
	var value []byte

	err := n.db.View(func(tx *nutsdb.Tx) error {
		result, err := tx.Get(DefaultBucket, key)
		value = append([]byte(nil), result...)
		return err
	})
	if errors.Is(err, nutsdb.ErrKeyNotFound) {
		return nil, ErrNotFoundKey
	}

	return value, err
}

func (n *NutsDB) Put(key, value []byte) error {
	return n.WriteBatch([]Entry{
		{
			Key:   key,
			Value: value,
		},
	})
}

func (n *NutsDB) WriteBatch(entries []Entry) error {
	return n.db.Update(func(tx *nutsdb.Tx) error {
		for _, entry := range entries {
			if err := tx.Put(DefaultBucket, entry.Key, entry.Value, 0); err != nil {
				return err
			}
		}

		return nil
	})
}

func (n *NutsDB) Scan(prefix []byte) ([]Entry, error) {
	var entries []Entry

	err := n.db.View(func(tx *nutsdb.Tx) error {
		keys, values, err := tx.GetAll(DefaultBucket)
		if err != nil {
			return err
		}

		for i, key := range keys {
			if bytes.HasPrefix(key, prefix) {
				entries = append(entries, Entry{append([]byte(nil), key...), append([]byte(nil), values[i]...)})
			}
		}

		return nil
	})
	return entries, err
}

func (n *NutsDB) Close() error {
	return n.db.Close()
}
