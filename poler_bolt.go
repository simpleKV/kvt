//go:build bolt
// +build bolt

package kvt

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type Bolt struct {
	tx *bolt.Tx
}

func NewPoler(t any) (*Bolt, error) {

	tx, ok := t.(*bolt.Tx)
	if !ok {
		return nil, fmt.Errorf(errNewPolerFailed)
	}
	return &Bolt{tx: tx}, nil
}

func (this *Bolt) CreateBucket(path []string) (err error) {

	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	case 1:
		_, err = this.tx.CreateBucketIfNotExists([]byte(path[0]))
	default:
		i, bkt := 1, this.tx.Bucket([]byte(path[0]))
		for ; i < len(path)-1 && bkt != nil; i++ {
			bkt = bkt.Bucket([]byte(path[i]))
		}
		if bkt == nil {
			return fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
		}
		_, err = bkt.CreateBucketIfNotExists([]byte(path[len(path)-1]))
	}

	return err
}

func (this *Bolt) DeleteBucket(path []string) error {
	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	case 1:
		return this.tx.DeleteBucket([]byte(path[len(path)-1]))
	default:
		i, bkt := 1, this.tx.Bucket([]byte(path[0]))
		for ; i < len(path)-1 && bkt != nil; i++ {
			bkt = bkt.Bucket([]byte(path[i]))
		}
		if bkt == nil {
			return fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
		}
		return bkt.DeleteBucket([]byte(path[len(path)-1]))
	}
}

type bucketer interface {
	Bucket([]byte) *bolt.Bucket
}

// open the targe index
func openBucket(bkt bucketer, path []string) (ret *bolt.Bucket) {
	for i := range path {
		ret = bkt.Bucket([]byte(path[i]))
		if ret == nil {
			return nil
		}
		bkt = ret
	}
	return ret
}

func (this *Bolt) Put(key, value []byte, path []string) error {
	b := openBucket(this.tx, path)
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.Put(key, value)
}

func (this *Bolt) Delete(key []byte, path []string) error {
	b := openBucket(this.tx, path)
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.Delete(key)
}

func (this *Bolt) Get(key []byte, path []string) (v []byte, err error) {

	b := openBucket(this.tx, path)
	if b == nil {
		return v, fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.Get(key), nil
}

func (this *Bolt) Query(prefix []byte, filter FilterFunc, path []string) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	b := openBucket(this.tx, path)
	if b == nil {
		return result, fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
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

func (this *Bolt) Sequence(path []string) (seq uint64, err error) {

	b := openBucket(this.tx, path)
	if b == nil {
		return seq, fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.Sequence(), nil
}

func (this *Bolt) NextSequence(path []string) (seq uint64, err error) {

	b := openBucket(this.tx, path)
	if b == nil {
		return seq, fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.NextSequence()
}

func (this *Bolt) SetSequence(path []string, seq uint64) (err error) {

	b := openBucket(this.tx, path)
	if b == nil {
		return fmt.Errorf(errBucketOpenFailed, path[len(path)-1])
	}
	return b.SetSequence(seq)
}
