//go:build buntdb
// +build buntdb

package kvt

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/tidwall/buntdb"
)

type bunt struct {
	tx *buntdb.Tx
}

func NewPoler(t any) (Poler, error) {
	tx, ok := t.(*buntdb.Tx)
	if !ok {
		return nil, fmt.Errorf(errNewPolerFailed)
	}
	return &bunt{tx: tx}, nil
}

// buntdb doesn't support bucket, just add bucket name befor all key
// "rootpath/to/bucket:", will add a key token tail
func (this *bunt) CreateBucket(path string) (prefix []byte, offset int, err error) {
	switch len(path) {
	case 0:
		return prefix, offset, fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		name, _, _ := splitPath(path)
		switch {
		case strings.HasPrefix(name, IDXPrefix):
		case strings.HasPrefix(name, MIDXPrefix):
		default:
			this.SetSequence(path, 0) //only data bucket init sequence
		}
		return []byte(path), len(path) + 1, nil
	}
}

// here we need add a ':' split after path
// suppose we have two idx bucket: idx_type, idx_type_status
// use need delete with path=idx_type, we may delete both if without a split
func (this *bunt) DeleteBucket(path string) error {

	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		del := func(spliter byte) error {
			var delkeys []string
			this.tx.AscendKeys(string(path)+string(spliter)+"*", func(k, v string) bool {
				delkeys = append(delkeys, k)
				return true
			})
			for _, k := range delkeys {

				if _, err := this.tx.Delete(k); err != nil {
					return err
				}
			}
			return nil
		}
		//delete sequence and its idx bkt
		if err := del(defaultPathJoiner); err != nil {
			return err
		}

		//delete bkt self
		return del(defaultKeyJoiner)
	}
}

func (this *bunt) put(path string, key, value []byte, spliter byte) error {
	realKey := path + string(spliter) + (string(key))
	_, _, err := this.tx.Set(realKey, string(value), nil)
	return err
}

func (this *bunt) Put(path string, key, value []byte) error {
	return this.put(path, key, value, defaultKeyJoiner)
}

func (this *bunt) Delete(path string, key []byte) error {
	realKey := path + string(defaultKeyJoiner) + (string(key))
	_, err := this.tx.Delete(realKey)
	return err
}

func (this *bunt) get(path string, key []byte, spliter byte) (value []byte, err error) {
	realKey := path + string(spliter) + (string(key))
	v, err := this.tx.Get(realKey)
	if err == buntdb.ErrNotFound {
		return value, nil
	}
	return []byte(v), err
}

func (this *bunt) Get(path string, key []byte) (value []byte, err error) {
	return this.get(path, key, defaultKeyJoiner)
}

func (this *bunt) Query(path string, prefix []byte, filter FilterFunc) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	realPrefix := path + string(defaultKeyJoiner) + string(prefix) + "*"
	err = this.tx.AscendKeys(realPrefix, func(key, value string) bool {

		if filter([]byte(key)) {
			result = append(result, KVPair{Key: []byte(key), Value: []byte(value)})
		}
		return true // continue iteration
	})

	return result, err
}

func (this *bunt) Sequence(path string) (seq uint64, err error) {

	v, err := this.get(path, []byte("sequence"), defaultPathJoiner)
	if err != nil {
		return 0, err
	}

	p := (*uint64)(Ptr((&v[0])))
	return *p, nil
}

func (this *bunt) NextSequence(path string) (seq uint64, err error) {

	s, err := this.Sequence(path)
	if err != nil {
		return s, err
	}
	s = s + 1
	return s, this.SetSequence(path, s)
}

func (this *bunt) SetSequence(path string, seq uint64) (err error) {
	return this.put(path, []byte("sequence"), Bytes(Ptr(&seq), unsafe.Sizeof(seq)), defaultPathJoiner)
}
