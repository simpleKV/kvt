//go:build boltdb
// +build boltdb

package kvt

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type boltdb struct {
	tx *bolt.Tx
}

func NewPoler(t any) (Poler, error) {

	tx, ok := t.(*bolt.Tx)
	if !ok {
		return nil, fmt.Errorf(errNewPolerFailed)
	}
	return &boltdb{tx: tx}, nil
}

// for boltdb, we disable nest idx bucket into main bkt
// just add main bkt name as prefix idx name
// like that:  bkt_main/idx_Type
func (this *boltdb) CreateBucket(path string) (prefix []byte, offset int, err error) {

	fmt.Println("create bkt:", path)
	if len(path) == 0 {
		return prefix, offset, fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	}

	prefix = []byte(path)
	_, err = this.tx.CreateBucketIfNotExists(prefix)
	return prefix, offset, err
}

func (this *boltdb) DeleteBucket(path string) error {
	if len(path) == 0 {
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	}

	return this.tx.DeleteBucket([]byte(path))
}

func (this *boltdb) Put(path string, key, value []byte) error {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.Put(key, value)
}

func (this *boltdb) Delete(path string, key []byte) error {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.Delete(key)
}

func (this *boltdb) Get(path string, key []byte) (v []byte, err error) {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return v, fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.Get(key), nil
}

func (this *boltdb) Query(path string, prefix []byte, filter FilterFunc) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return result, fmt.Errorf(errBucketOpenFailed, path)
	}
	c := b.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if !filter(k) {
			continue
		}
		result = append(result, KVPair{Key: k, Value: v})
	}

	return result, nil
}

func (this *boltdb) Sequence(path string) (seq uint64, err error) {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return seq, fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.Sequence(), nil
}

func (this *boltdb) NextSequence(path string) (seq uint64, err error) {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return seq, fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.NextSequence()
}

func (this *boltdb) SetSequence(path string, seq uint64) (err error) {
	b := this.tx.Bucket([]byte(path))
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path)
	}
	return b.SetSequence(seq)
}
