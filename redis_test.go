//go:build redis
// +build redis

package kvt

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func Test_queryEqualAllString(t *testing.T) {

	type Order struct {
		ID       string
		Type     string
		Status   uint16
		Name     string
		District string
		Num      int
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		test, _ := obj.(*Order)
		enc.Encode(test)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &Order{}
		dec.Decode(test)
		return test, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		test, _ := obj.(*Order)
		return []byte(test.ID), nil
	}

	// generate key of idx_Type
	idx_Type := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*Order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type)) //every index should append primary key at end
		return key, nil
	}
	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_Order",
		Marshal:   valueEncode,
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "Bucket_Order/idx_Type"},
				idx_Type,
			},
			{
				&IndexInfo{Name: "pk_ID"},
				pk_ID,
			},
		},
	}

	k, err := New(Order{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.DeleteIndexBuckets(p)
		k.DeleteDataBucket(p)
		return nil
	})

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.CreateDataBucket(p)
		//k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	odInputs := []Order{
		Order{
			ID:       "1001",
			Type:     "book",
			Status:   1,
			Name:     "Alice",
			District: "East ST",
		},
		Order{
			ID:       "1002",
			Type:     "fruit",
			Status:   2,
			Name:     "Bob",
			District: "South ST",
		},
		Order{
			ID:       "1003",
			Type:     "fruit",
			Status:   3,
			Name:     "Carl",
			District: "West ST",
		},
		Order{
			ID:       "1004",
			Type:     "book",
			Status:   2,
			Name:     "Dicken",
			District: "East ST",
		},
	}
	bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		for i := range odInputs {
			//odInputs[i].ID, _ = k.NextSequence(p)
			fmt.Println("put ", odInputs[i])
			if err = k.Put(p, &odInputs[i]); err != nil {
				t.Errorf("put kvt fail: %s", err)
				return err
			}
		}
		return nil
	})

	cmpResult := func(result []any, err error, ords map[string]Order) {
		fmt.Println("err:", err, "len result:", len(result), len(ords))
		if err != nil || len(result) != len(ords) {
			t.Errorf("got query result fail")
		}
		for i := range result {
			odd, _ := result[i].(*Order)
			if !reflect.DeepEqual(*odd, ords[odd.ID]) {
				t.Errorf("not found id %s", odd.ID)
				fmt.Println("odd:", odd)
			}
		}
	}

	qi := QueryInfo{
		IndexName: "idx_Type",
		Where: map[string][]byte{
			"Type": []byte(odInputs[1].Type),
		},
	}

	p := NewRedisPoler(bdb, nil, ctx)
	r, err := k.Query(p, qi)
	cmpResult(r, err, map[string]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})

	fmt.Println("################################")

}

func Test_queryEqual(t *testing.T) {

	type Order struct {
		ID       uint64
		Type     string
		Status   uint16
		Name     string
		District string
		Num      int
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		test, _ := obj.(*Order)
		enc.Encode(test)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &Order{}
		dec.Decode(test)
		return test, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		test, _ := obj.(*Order)
		return Bytes(Ptr(&test.ID), unsafe.Sizeof(test.ID)), nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status_District := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*Order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status)),
			[]byte(test.District)) //every index should append primary key at end
		return key, nil
	}
	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_Order",
		Marshal:   valueEncode,
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "Bucket_Order/idx_Type_Status_District"},
				idx_Type_Status_District,
			},
			{
				&IndexInfo{Name: "pk_ID"},
				pk_ID,
			},
		},
	}

	k, err := New(Order{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Del(ctx, kp.Bucket)
	bdb.Del(ctx, kp.Indexs[0].Name)

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	odInputs := []Order{
		Order{
			Type:     "book",
			Status:   1,
			Name:     "Alice",
			District: "East ST",
		},
		Order{
			Type:     "fruit",
			Status:   2,
			Name:     "Bob",
			District: "South ST",
		},
		Order{
			Type:     "fruit",
			Status:   3,
			Name:     "Carl",
			District: "West ST",
		},
		Order{
			Type:     "book",
			Status:   2,
			Name:     "Dicken",
			District: "East ST",
		},
	}
	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		for i := range odInputs {
			odInputs[i].ID, _ = k.NextSequence(p)
			fmt.Println("put ", odInputs[i])
			if err = k.Put(p, &odInputs[i]); err != nil {
				t.Errorf("put kvt fail: %s", err)
				return err
			}
		}
		return nil
	})

	cmpResult := func(result []any, err error, ords map[uint64]Order) {
		fmt.Println("err:", err, "len result:", len(result), len(ords))
		if err != nil || len(result) != len(ords) {
			t.Errorf("got query result fail")
		}
		for i := range result {
			odd, _ := result[i].(*Order)
			if !reflect.DeepEqual(*odd, ords[odd.ID]) {
				t.Errorf("not found id %d", odd.ID)
				fmt.Println("odd:", odd)
			}
		}
	}

	qi := QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type": []byte(odInputs[1].Type),
		},
	}
	p := NewRedisPoler(bdb, nil, ctx)
	r, err := k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})

	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type":     []byte(odInputs[1].Type),
			"Status":   Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
			"District": []byte(odInputs[1].District),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1]})

	//query by fruit, should be 2 order
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type": []byte("fruit"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			//"District": []byte(od.District),
		},
	}
	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})

	//partial query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type": []byte("book"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			"District": []byte("East ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})

	//empty prefix query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			"District": []byte("East ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})

	//empty prefix query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Status": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
			//"District": []byte("West ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[3].ID: odInputs[3]})

}

func Test_queryRange(t *testing.T) {

	type Order struct {
		ID       uint64
		Type     string
		Status   uint16
		Name     string
		District string
		Num      int
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		test, _ := obj.(*Order)
		enc.Encode(test)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &Order{}
		dec.Decode(test)
		return test, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		test, _ := obj.(*Order)
		return Bytes(Ptr(&test.ID), unsafe.Sizeof(test.ID)), nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status_District := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*Order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status)),
			[]byte(test.District)) //every index should append primary key at end
		return key, nil
	}

	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_Order",
		Marshal:   valueEncode,
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "idx_Type_Status_District"},
				idx_Type_Status_District,
			},
			{
				&IndexInfo{Name: "pk_ID"},
				pk_ID,
			},
		},
	}

	k, err := New(Order{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}
	bdb.Del(ctx, kp.Bucket)
	bdb.Del(ctx, kp.Indexs[0].Name)

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	odInputs := []Order{
		Order{
			Type:     "book",
			Status:   1,
			Name:     "Alice",
			District: "East ST",
		},
		Order{
			Type:     "fruit",
			Status:   2,
			Name:     "Bob",
			District: "South ST",
		},
		Order{
			Type:     "fruit",
			Status:   3,
			Name:     "Carl",
			District: "West ST",
		},
		Order{
			Type:     "book",
			Status:   2,
			Name:     "Dicken",
			District: "East ST",
		},
		Order{
			Type:     "fruit",
			Status:   4,
			Name:     "Frank",
			District: "East ST",
		},
	}

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		for i := range odInputs {
			odInputs[i].ID, _ = k.NextSequence(p)
			fmt.Println("put ", odInputs[i])
			if err = k.Put(p, &odInputs[i]); err != nil {
				t.Errorf("put kvt fail: %s", err)
			}
		}
		return nil
	})

	cmpResult := func(result []any, err error, ords map[uint64]Order) {

		if err != nil || len(result) != len(ords) {
			t.Errorf("got query result fail")
		}
		for i := range result {
			odd, _ := result[i].(*Order)
			if !reflect.DeepEqual(*odd, ords[odd.ID]) {
				t.Errorf("not found id %d", odd.ID)
				fmt.Println("odd:", odd)
			}
		}
	}

	rqi := RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type":   map[string][]byte{"=": []byte("book")},
			"Status": map[string][]byte{"==": Bytes(Ptr(&odInputs[0].Status), unsafe.Sizeof(odInputs[0].Status))},
		},
	}
	p := NewRedisPoler(bdb, nil, ctx)
	r, err := k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0]})

	// Status > 2
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type":   map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{">": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status))},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[4].ID: odInputs[4]})

	//Status >= 2
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type":   map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{">=": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status))},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1], odInputs[4].ID: odInputs[4]})

	// 2 <= Status < 4
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type": map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{
				">=": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
				"<":  Bytes(Ptr(&odInputs[4].Status), unsafe.Sizeof(odInputs[4].Status)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1]})

	// 3 <= Status && Status == 3
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type": map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{
				">=": Bytes(Ptr(&odInputs[2].Status), unsafe.Sizeof(odInputs[2].Status)),
				"=":  Bytes(Ptr(&odInputs[2].Status), unsafe.Sizeof(odInputs[2].Status)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2]})

	//partial query
	// 1 < Status && Status <=4
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			//"Type": map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{
				">":  Bytes(Ptr(&odInputs[0].Status), unsafe.Sizeof(odInputs[0].Status)),
				"<=": Bytes(Ptr(&odInputs[4].Status), unsafe.Sizeof(odInputs[4].Status)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2], odInputs[3].ID: odInputs[3], odInputs[4].ID: odInputs[4]})

	// type < "fruit" and status > 1
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type": map[string][]byte{"<": []byte("fruit")},
			"Status": map[string][]byte{
				">": Bytes(Ptr(&odInputs[0].Status), unsafe.Sizeof(odInputs[0].Status)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Order{odInputs[3].ID: odInputs[3]})

}

func Test_queryTimeRange(t *testing.T) {

	type People struct {
		ID    uint64
		Name  string
		Birth time.Time
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		p, _ := obj.(*People)
		enc.Encode(p)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		var p *People
		if obj != nil {
			p = obj.(*People)
		} else {
			p = &People{}
		}
		dec.Decode(p)
		return p, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		p, _ := obj.(*People)
		return Bytes(Ptr(&p.ID), unsafe.Sizeof(p.ID)), nil
	}

	// generate key of idx_Type_Status
	idx_Birth := func(obj interface{}) ([]byte, error) {
		p, _ := obj.(*People)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(p.Birth.Format(time.RFC3339))) //every index should append primary key at end
		return key, nil
	}
	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_People",
		Marshal:   valueEncode,
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "idx_Birth"},
				idx_Birth,
			},
			{
				&IndexInfo{Name: "pk_ID"},
				pk_ID,
			},
		},
	}

	k, err := New(People{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Del(ctx, kp.Bucket)
	bdb.Del(ctx, "Bucket_People/idx_Birth")

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	ps := []People{
		People{
			Name:  "Alice",
			Birth: time.Now(),
		},
		People{
			Name:  "Bob",
			Birth: time.Now().Add(time.Hour * 1),
		},
		People{
			Name:  "Carl",
			Birth: time.Date(2009, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		for i := range ps {
			ps[i].ID, _ = k.NextSequence(p)
			fmt.Println(ps[i])
			k.Put(p, &ps[i])
		}
		return nil
	})

	pepoleEqual := func(p1 People, p2 People) bool {
		return p1.ID == p2.ID && p1.Name == p2.Name && p1.Birth.Format(time.RFC3339) == p1.Birth.Format(time.RFC3339)
	}
	cmpResult := func(result []any, err error, pm map[uint64]People) {
		fmt.Println(err, len(result), len(pm))
		if err != nil || len(result) != len(pm) {
			t.Errorf("got query result fail %d %d", len(result), len(pm))
		}
		for i := range result {
			p, _ := result[i].(*People)
			if !pepoleEqual(*p, pm[p.ID]) {
				t.Errorf("not found id %d", p.ID)
			}
		}
	}

	rqi := RangeInfo{
		IndexName: "idx_Birth",
		Where: map[string]map[string][]byte{
			"Birth": map[string][]byte{
				"=": []byte(ps[2].Birth.Format(time.RFC3339)),
			},
		},
	}

	p := NewRedisPoler(bdb, nil, ctx)
	r, err := k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]People{ps[2].ID: ps[2]})

	rqi = RangeInfo{
		IndexName: "idx_Birth",
		Where: map[string]map[string][]byte{
			"Birth": map[string][]byte{
				">": []byte(ps[0].Birth.Format(time.RFC3339)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]People{ps[1].ID: ps[1]})

	rqi = RangeInfo{
		IndexName: "idx_Birth",
		Where: map[string]map[string][]byte{
			"Birth": map[string][]byte{
				"<": []byte(time.Now().Add(time.Minute * 1).Format(time.RFC3339)),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]People{ps[0].ID: ps[0], ps[2].ID: ps[2]})

}

func Test_queryMIndex(t *testing.T) {

	type Book struct {
		ID    uint64
		Name  string
		Type  string
		Tags  []string
		Level int
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		p, _ := obj.(*Book)
		enc.Encode(p)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		var p *Book
		if obj != nil {
			p = obj.(*Book)
		} else {
			p = &Book{}
		}
		dec.Decode(p)
		return p, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		p, _ := obj.(*Book)
		return Bytes(Ptr(&p.ID), unsafe.Sizeof(p.ID)), nil
	}

	// generate key of idx_Type
	idx_Type := func(obj interface{}) ([]byte, error) {
		p, _ := obj.(*Book)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(p.Type)) //every index should append primary key at end
		return key, nil
	}

	midx_Level_Tag := func(obj interface{}) (ret [][]byte, err error) {
		p, _ := obj.(*Book)
		for i := range p.Tags {
			key := MakeIndexKey(make([]byte, 0, 20),
				Bytes(Ptr(&p.Level), unsafe.Sizeof(p.Level)),
				[]byte(p.Tags[i])) //every index should append primary key at end
			ret = append(ret, key)
			fmt.Println("idx Tag:", key)
		}
		return ret, nil
	}
	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_Book",
		Marshal:   valueEncode,
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "Bucket_Book/idx_Type"},
				idx_Type,
			},
			{
				&IndexInfo{Name: "pk_ID"},
				pk_ID,
			},
		},
		MIndexs: []MIndex{
			{
				&IndexInfo{
					Name:   "Bucket_Book/midx_Level_Tags",
					Fields: []string{"Level", "Tags"},
				},
				midx_Level_Tag,
			},
		},
	}

	k, err := New(Book{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Del(ctx, kp.Bucket)
	bdb.Del(ctx, kp.Indexs[0].Name)
	bdb.Del(ctx, kp.MIndexs[0].Name)

	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	ps := []Book{
		Book{
			Name:  "Alice",
			Type:  "travel",
			Tags:  []string{"aa", "AA", "xyz"},
			Level: 3,
		},
		Book{
			Name:  "Bible",
			Type:  "dictionary",
			Tags:  []string{"bb", "BB", "xyzz"},
			Level: 5,
		},
		Book{
			Name:  "Cat",
			Type:  "animal",
			Tags:  []string{"cc", "CC", "xyz"},
			Level: 2,
		},
	}
	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		for i := range ps {
			ps[i].ID, _ = k.NextSequence(p)
			fmt.Println(ps[i])
			k.Put(p, &ps[i])
		}
		return nil
	})

	cmpArray := func(s1, s2 []string) bool {
		if len(s1) != len(s2) {
			return false
		}
		for i := range s1 {
			found := false
			for j := range s2 {
				if s1[i] == s2[j] {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	bookEqual := func(p1 Book, p2 Book) bool {
		return p1.ID == p2.ID && p1.Name == p2.Name && p1.Type == p2.Type && cmpArray(p1.Tags, p2.Tags)
	}
	cmpResult := func(result []any, err error, pm map[uint64]Book) {
		fmt.Println(err, len(result), len(pm))
		if err != nil || len(result) != len(pm) {
			t.Errorf("got query result fail %d %d", len(result), len(pm))
		}
		for i := range result {
			p, _ := result[i].(*Book)
			if !bookEqual(*p, pm[p.ID]) {
				t.Errorf("not found id %d", p.ID)
			}
		}
	}

	rqi := RangeInfo{
		IndexName: "idx_Type",
		Where: map[string]map[string][]byte{
			"Type": map[string][]byte{
				"=": []byte("animal"),
			},
		},
	}
	p := NewRedisPoler(bdb, nil, ctx)
	r, err := k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Book{ps[2].ID: ps[2]})

	rqi = RangeInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string]map[string][]byte{
			"Level": map[string][]byte{
				"=": Bytes(Ptr(&ps[1].Level), unsafe.Sizeof(ps[1].Level)),
			},
			"Tags": map[string][]byte{
				"=": []byte("bb"), //one of tags is "bb"
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Book{ps[1].ID: ps[1]})

	rqi = RangeInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string]map[string][]byte{
			"Tags": map[string][]byte{
				"=": []byte("xyz"), //one of tags is "bb"
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	fmt.Println(err, r, len(r))
	cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[2].ID: ps[2]})

	rqi = RangeInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string]map[string][]byte{
			"Level": map[string][]byte{
				">": Bytes(Ptr(&ps[2].Level), unsafe.Sizeof(ps[2].Level)), //>2
			},
			"Tags": map[string][]byte{
				"=": []byte("xyzz"),
			},
		},
	}

	r, err = k.RangeQuery(p, rqi)
	cmpResult(r, err, map[uint64]Book{ps[1].ID: ps[1]})

	qi := QueryInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Tags": []byte("xyz"),
			//"District": []byte("West ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[2].ID: ps[2]})

	//add a new tag for ps[1]
	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		ps[1].Tags = append(ps[1].Tags, "xyz")
		k.Put(p, &ps[1])
		return nil
	})

	qi = QueryInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Tags": []byte("xyz"),
			//"District": []byte("West ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[1].ID: ps[1], ps[2].ID: ps[2]})

	//change tags of ps[2], remove "xyz"
	_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := NewRedisPoler(bdb, pipe, ctx)
		ps[2].Tags = []string{"c", "CC"}
		k.Put(p, &ps[2])
		return nil
	})

	qi = QueryInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Tags": []byte("xyz"),
			//"District": []byte("West ST"),
		},
	}

	r, err = k.Query(p, qi)
	cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[1].ID: ps[1]})

}

func Test_nestBucket(t *testing.T) {

	type Order struct {
		ID     uint64
		Type   string
		Status uint16
	}

	valueEncode := func(obj any) ([]byte, error) {
		var network bytes.Buffer // Stand-in for the network.
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)

		test, _ := obj.(*Order)
		enc.Encode(test)

		return network.Bytes(), nil
	}
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &Order{}
		dec.Decode(test)
		return test, nil
	}

	pk_ID := func(obj any) ([]byte, error) {
		test, _ := obj.(*Order)
		return Bytes(Ptr(&test.ID), unsafe.Sizeof(test.ID)), nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*Order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status))) //every index should append primary key at end
		return key, nil
	}
	bdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer bdb.Close()

	initkvt := func(mainBucket, idxBucket, idxName string, fields []string) {

		kp := KVTParam{
			Bucket:    mainBucket,
			Marshal:   valueEncode,
			Unmarshal: valueDecode,
			Indexs: []Index{
				{
					&IndexInfo{
						Name:   idxBucket,
						Fields: fields,
					},
					idx_Type_Status,
				},
				{
					&IndexInfo{Name: "pk_ID"},
					pk_ID,
				},
			},
		}

		k, err := New(Order{}, &kp)
		if err != nil {
			t.Errorf("new kvt fail: %s", err)
			return
		}

		od := Order{uint64(rand.Int63()), "book", 1}
		_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			p := NewRedisPoler(bdb, pipe, ctx)
			k.CreateDataBucket(p)
			k.SetSequence(p, 1000)
			k.CreateIndexBuckets(p)
			k.Put(p, &od)
			return nil
		})

		qi := QueryInfo{
			IndexName: idxName,
			Where: map[string][]byte{
				"Type":   []byte(od.Type),
				"Status": Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			},
		}
		p := NewRedisPoler(bdb, nil, ctx)
		r, err := k.Query(p, qi)
		if err != nil || len(r) != 1 {
			t.Errorf("query order fail")
			fmt.Println("query order fail:", mainBucket, idxBucket, err, len(r))
		}
		o := r[0].(*Order)
		if !reflect.DeepEqual(od, *o) {
			t.Errorf("query order fail not equal")
			fmt.Println("query order not equal: ", mainBucket, idxBucket, od, *o)
		}

		_, err = bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			p := NewRedisPoler(bdb, pipe, ctx)
			k.Delete(p, &od)
			return nil
		})
	}

	initkvt("bkt_Order", "idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("bkt_Order1", "bkt_Order1/idx_Type_Status", "idx_Type_Status", []string{})

	initkvt("a/bkt_Order1", "idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("bkt_Order2", "idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("bkt_Order3", "a/idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("bkt_Order4", "a/idx_Type_Statuszyz", "idx_Type_Statuszyz", []string{"Type", "Status"})
	initkvt("bkt_Order5", "bkt_Order5/idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("bkt_Order6", "a/b/idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("a/bkt_Order7", "a/b/idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("a/b/bkt_Order8", "a/idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("a/b/bkt_Order9", "idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("a/b/bkt_Order10", "bkt_Order10/idx_Type_Statusaaa", "idx_Type_Statusaaa", []string{"Type", "Status"})
	initkvt("a/b/bkt_Order11", "idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("a/b/bkt_Order12", "a/idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("a/b/bkt_Order13", "a/b/idx_Type_Status", "idx_Type_Status", []string{})
	initkvt("a/b/bkt_Order14", "bkt_Order14/idx_Type_Status", "idx_Type_Status", []string{})
}