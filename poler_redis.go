//go:build redis
// +build redis

package kvt

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

const errRedisNil = "redis: nil"
const sequenceName = "_sequence_"

type redisdb struct {
	rdb  *redis.Client
	pipe redis.Pipeliner
	ctx  context.Context
}

func NewRedisPoler(cli *redis.Client, p redis.Pipeliner, ct context.Context) Poler {
	return &redisdb{rdb: cli, pipe: p, ctx: ct}
}

// redis needn't create bucket, just hset under the bkt key
// prefix is the full bkt key, offset is 0 for redis
func (this *redisdb) CreateBucket(path string) (prefix []byte, offset int, err error) {
	switch len(path) {
	case 0:
		return prefix, offset, fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		name, _, _ := splitPath(path)
		switch {
		case strings.HasPrefix(name, IDXPrefix):
		case strings.HasPrefix(name, MIDXPrefix):
		default:
			this.SetSequence(path, 0) //need init sequence
		}

		return []byte(path), offset, nil
	}
}

func (this *redisdb) DeleteBucket(path string) error {
	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		this.pipe.Del(this.ctx, path)
		return nil
	}
}

func (this *redisdb) Put(path string, key, value []byte) error {
	status := this.pipe.HSet(this.ctx, path, string(key), value)
	_, err := status.Result()
	return err
}

func (this *redisdb) Delete(path string, key []byte) error {
	intCmd := this.pipe.HDel(this.ctx, path, string(key))
	_, err := intCmd.Result()
	return err
}

func (this *redisdb) Get(path string, key []byte) (value []byte, err error) {
	sCmd := this.rdb.HGet(this.ctx, path, string(key))
	v, err := sCmd.Bytes()
	if err != nil && err.Error() == errRedisNil {
		return v, nil
	}
	return v, err
}

func (this *redisdb) Query(path string, prefix []byte, filter FilterFunc) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	realPrefix := string(prefix) + "*"
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = this.rdb.HScan(this.ctx, path, cursor, realPrefix, 100).Result()
		if err != nil {
			panic(err)
		}
		for i := 0; i < len(keys); i += 2 {
			if filter([]byte(keys[i])) && keys[i] != sequenceName {
				result = append(result, KVPair{Key: []byte(keys[i]), Value: []byte(keys[i+1])})
			}
		}
		if cursor == 0 {
			break
		}
	}

	return result, err
}

func (this *redisdb) Sequence(path string) (seq uint64, err error) {

	scmd := this.rdb.HGet(this.ctx, path, sequenceName)
	s, err := scmd.Bytes()

	p := (*uint64)(Ptr((&s[0])))
	return *p, nil
}

func (this *redisdb) NextSequence(path string) (seq uint64, err error) {
	icmd := this.rdb.HIncrBy(this.ctx, path, sequenceName, 1)
	ret, err := icmd.Result()
	return uint64(ret), err
}

func (this *redisdb) SetSequence(path string, seq uint64) (err error) {
	icmd := this.rdb.HSet(this.ctx, path, sequenceName, seq)
	_, err = icmd.Result()
	return err
}
