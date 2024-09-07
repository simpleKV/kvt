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
func (this *redisdb) CreateBucket(path []string) (prefix []byte, offset int, err error) {
	switch len(path) {
	case 0:
		return prefix, offset, fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		p := strings.Join(path, string(defaultPathJoiner))
		switch {
		case strings.HasPrefix(path[len(path)-1], IDXPrefix):
		case strings.HasPrefix(path[len(path)-1], MIDXPrefix):
		default:
			this.SetSequence(path, 0) //need init sequence
		}

		return []byte(p), offset, nil
	}
}

func (this *redisdb) DeleteBucket(path []string) error {
	switch len(path) {
	case 0:
		return fmt.Errorf(errBucketOpenFailed, "empty bucket name")
	default:
		this.pipe.Del(this.ctx, path[0])
		return nil
	}
}

func (this *redisdb) Put(key, value []byte, path []string) error {
	status := this.pipe.HSet(this.ctx, path[0], string(key), value)
	_, err := status.Result()
	return err
}

func (this *redisdb) Delete(key []byte, path []string) error {
	intCmd := this.pipe.HDel(this.ctx, path[0], string(key))
	_, err := intCmd.Result()
	return err
}

func (this *redisdb) Get(key []byte, path []string) (value []byte, err error) {
	sCmd := this.rdb.HGet(this.ctx, path[0], string(key))
	v, err := sCmd.Bytes()
	if err != nil && err.Error() == errRedisNil {
		return v, nil
	}
	return v, err
}

func (this *redisdb) Query(prefix []byte, filter FilterFunc, path []string) (result []KVPair, err error) {
	result = make([]KVPair, 0)

	realPrefix := string(prefix) + "*"
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = this.rdb.HScan(this.ctx, path[0], cursor, realPrefix, 100).Result()
		if err != nil {
			panic(err)
		}
		for i := 0; i < len(keys); i += 2 {
			if filter([]byte(keys[i])) {
				result = append(result, KVPair{Key: []byte(keys[i]), Value: []byte(keys[i+1])})
			}
		}
		if cursor == 0 {
			break
		}
	}

	return result, err
}

func (this *redisdb) Sequence(path []string) (seq uint64, err error) {

	scmd := this.rdb.HGet(this.ctx, path[0], "sequence")
	s, err := scmd.Bytes()

	p := (*uint64)(Ptr((&s[0])))
	return *p, nil
}

func (this *redisdb) NextSequence(path []string) (seq uint64, err error) {
	icmd := this.rdb.HIncrBy(this.ctx, path[0], "sequence", 1)
	ret, err := icmd.Result()
	return uint64(ret), err
}

func (this *redisdb) SetSequence(path []string, seq uint64) (err error) {
	icmd := this.rdb.HSet(this.ctx, path[0], "sequence", seq)
	_, err = icmd.Result()
	return err
}
