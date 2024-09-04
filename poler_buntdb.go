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
func (this *bunt) CreateBucket(path []string) (prefix []byte, err error) {
	switch len(path) {
	case 0:
		return prefix, fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		p := strings.Join(path, string(defaultPathJoiner))
		this.SetSequence(path, 0) //need init sequence
		return []byte(p + string(defaultKeyJoiner)), nil
	}

}

func (this *bunt) DeleteBucket(path []string) error {
	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		var delkeys []string
		this.tx.AscendKeys(string(path[0]), func(k, v string) bool {
			delkeys = append(delkeys, k)
			return true // continue
		})
		for _, k := range delkeys {
			if _, err := this.tx.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}
}

func (this *bunt) Put(key, value []byte, path []string) error {
	realKey := path[0] + (string(key))
	_, _, err := this.tx.Set(realKey, string(value), nil)
	return err
}

func (this *bunt) Delete(key []byte, path []string) error {
	realKey := path[0] + (string(key))
	_, err := this.tx.Delete(realKey)
	return err
}

func (this *bunt) Get(key []byte, path []string) (value []byte, err error) {
	realKey := path[0] + (string(key))
	v, err := this.tx.Get(realKey)
	if err == buntdb.ErrNotFound {
		return value, nil
	}
	return []byte(v), err
}

func (this *bunt) Query(prefix []byte, filter FilterFunc, path []string) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	realPrefix := path[0] + string(prefix) + "*"
	err = this.tx.AscendKeys(realPrefix, func(key, value string) bool {
		if filter([]byte(key)) {
			result = append(result, KVPair{Key: []byte(key), Value: []byte(value)})
		}
		return true // continue iteration
	})

	return result, err
}

func (this *bunt) Sequence(path []string) (seq uint64, err error) {

	v, err := this.Get([]byte(":sequence"), path)
	if err != nil {
		return 0, err
	}

	p := (*uint64)(Ptr((&v[0])))
	return *p, nil
}

func (this *bunt) NextSequence(path []string) (seq uint64, err error) {

	s, err := this.Sequence(path)
	if err != nil {
		return s, err
	}
	s = s + 1
	return s, this.SetSequence(path, s)
}

func (this *bunt) SetSequence(path []string, seq uint64) (err error) {
	return this.Put([]byte(":sequence"), Bytes(Ptr(&seq), unsafe.Sizeof(seq)), path)
}
