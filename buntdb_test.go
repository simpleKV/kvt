//go:build buntdb
// +build buntdb

package kvt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/tidwall/buntdb"
)

func Test_queryEqual(t *testing.T) {
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &order{}
		dec.Decode(test)
		return test, nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status_District := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status)),
			[]byte(test.District)) //every index should append primary key at end
		return key, nil
	}
	os.Remove("query_test.bdb")
	bdb, err := buntdb.Open("query_test.bdb")
	if err != nil {
		return
	}
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_order",
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "Bucket_order/idx_Type_Status_District"},
				idx_Type_Status_District,
			},
		},
	}

	k, err := New(order{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	odInputs := []order{
		order{
			Type:     "book",
			Status:   1,
			Name:     "Alice",
			District: "East ST",
		},
		order{
			Type:     "fruit",
			Status:   2,
			Name:     "Bob",
			District: "South ST",
		},
		order{
			Type:     "fruit",
			Status:   3,
			Name:     "Carl",
			District: "West ST",
		},
		order{
			Type:     "book",
			Status:   2,
			Name:     "Dicken",
			District: "East ST",
		},
	}
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
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

	bdb.View(func(tx *buntdb.Tx) error {
		err := tx.Ascend("", func(key, value string) bool {
			fmt.Printf("List key: %s, value: %s\n", key, value)
			return true // continue iteration
		})
		return err
	})

	cmpResult := func(result []any, err error, ords map[uint64]order) {
		fmt.Println("err:", err, "len result:", len(result), len(ords))
		if err != nil || len(result) != len(ords) {
			t.Errorf("got query result fail")
		}
		for i := range result {
			odd, _ := result[i].(*order)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})
		return nil
	})

	fmt.Println("################################")

	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type":     []byte(odInputs[1].Type),
			"Status":   Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
			"District": []byte(odInputs[1].District),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[1].ID: odInputs[1]})
		return nil
	})

	//query by fruit, should be 2 order
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type": []byte("fruit"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			//"District": []byte(od.District),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})
		return nil
	})

	//partial query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			"Type": []byte("book"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			"District": []byte("East ST"),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})
		return nil
	})

	//empty prefix query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			//"Status":   Bytes(Ptr(&od.Status), unsafe.Sizeof(od.Status)),
			"District": []byte("East ST"),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})
		return nil
	})

	//empty prefix query
	qi = QueryInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Status": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
			//"District": []byte("West ST"),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]order{odInputs[1].ID: odInputs[1], odInputs[3].ID: odInputs[3]})
		return nil
	})
}

func Test_queryRange(t *testing.T) {
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &order{}
		dec.Decode(test)
		return test, nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status_District := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*order)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status)),
			[]byte(test.District)) //every index should append primary key at end
		return key, nil
	}

	os.Remove("query_test.bdb")
	bdb, err := buntdb.Open("query_test.bdb")
	if err != nil {
		return
	}
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_order",
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "idx_Type_Status_District"},
				idx_Type_Status_District,
			},
		},
	}

	k, err := New(order{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	odInputs := []order{
		order{
			Type:     "book",
			Status:   1,
			Name:     "Alice",
			District: "East ST",
		},
		order{
			Type:     "fruit",
			Status:   2,
			Name:     "Bob",
			District: "South ST",
		},
		order{
			Type:     "fruit",
			Status:   3,
			Name:     "Carl",
			District: "West ST",
		},
		order{
			Type:     "book",
			Status:   2,
			Name:     "Dicken",
			District: "East ST",
		},
		order{
			Type:     "fruit",
			Status:   4,
			Name:     "Frank",
			District: "East ST",
		},
	}
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		for i := range odInputs {
			odInputs[i].ID, _ = k.NextSequence(p)
			fmt.Println("put ", odInputs[i])
			if err = k.Put(p, &odInputs[i]); err != nil {
				t.Errorf("put kvt fail: %s", err)
			}
		}
		return nil
	})

	cmpResult := func(result []any, err error, ords map[uint64]order) {
		if err != nil || len(result) != len(ords) {
			t.Errorf("got query result fail")
		}
		for i := range result {
			odd, _ := result[i].(*order)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[0].ID: odInputs[0]})
		return nil
	})

	// Status > 2
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type":   map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{">": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status))},
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[2].ID: odInputs[2], odInputs[4].ID: odInputs[4]})
		return nil
	})

	//Status >= 2
	rqi = RangeInfo{
		IndexName: "idx_Type_Status_District",
		Where: map[string]map[string][]byte{
			"Type":   map[string][]byte{"=": []byte("fruit")},
			"Status": map[string][]byte{">=": Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status))},
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1], odInputs[4].ID: odInputs[4]})
		return nil
	})

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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1]})
		return nil
	})

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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[2].ID: odInputs[2]})
		return nil
	})

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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2], odInputs[3].ID: odInputs[3], odInputs[4].ID: odInputs[4]})
		return nil
	})

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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]order{odInputs[3].ID: odInputs[3]})
		return nil
	})
}

func Test_queryTimeRange(t *testing.T) {
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		var p *people
		if obj != nil {
			p = obj.(*people)
		} else {
			p = &people{}
		}
		dec.Decode(p)
		return p, nil
	}
	// generate key of idx_Type_Status
	idx_Birth := func(obj interface{}) ([]byte, error) {
		p, _ := obj.(*people)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(p.Birth.Format(time.RFC3339))) //every index should append primary key at end
		return key, nil
	}
	os.Remove("query_test.bdb")
	bdb, err := buntdb.Open("query_test.bdb")
	if err != nil {
		return
	}
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_People",
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "idx_Birth"},
				idx_Birth,
			},
		},
	}

	k, err := New(people{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	ps := []people{
		people{
			Name:  "Alice",
			Birth: time.Now(),
		},
		people{
			Name:  "Bob",
			Birth: time.Now().Add(time.Hour * 1),
		},
		people{
			Name:  "Carl",
			Birth: time.Date(2009, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		for i := range ps {
			ps[i].ID, _ = k.NextSequence(p)
			fmt.Println(ps[i])
			k.Put(p, &ps[i])
		}
		return nil
	})

	pepoleEqual := func(p1 people, p2 people) bool {
		return p1.ID == p2.ID && p1.Name == p2.Name && p1.Birth.Format(time.RFC3339) == p1.Birth.Format(time.RFC3339)
	}
	cmpResult := func(result []any, err error, pm map[uint64]people) {
		if err != nil || len(result) != len(pm) {
			t.Errorf("got query result fail %d %d", len(result), len(pm))
		}
		for i := range result {
			p, _ := result[i].(*people)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]people{ps[2].ID: ps[2]})
		return nil
	})

	rqi = RangeInfo{
		IndexName: "idx_Birth",
		Where: map[string]map[string][]byte{
			"Birth": map[string][]byte{
				">": []byte(ps[0].Birth.Format(time.RFC3339)),
			},
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]people{ps[1].ID: ps[1]})
		return nil
	})

	rqi = RangeInfo{
		IndexName: "idx_Birth",
		Where: map[string]map[string][]byte{
			"Birth": map[string][]byte{
				"<": []byte(time.Now().Add(time.Minute * 1).Format(time.RFC3339)),
			},
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]people{ps[0].ID: ps[0], ps[2].ID: ps[2]})
		return nil
	})
}

func Test_queryMIndex(t *testing.T) {
	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		var p *book
		if obj != nil {
			p = obj.(*book)
		} else {
			p = &book{}
		}
		dec.Decode(p)
		return p, nil
	}

	// generate key of idx_Type
	idx_Type := func(obj interface{}) ([]byte, error) {
		p, _ := obj.(*book)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(p.Type)) //every index should append primary key at end
		return key, nil
	}

	midx_Level_Tag := func(obj interface{}) (ret [][]byte, err error) {
		p, _ := obj.(*book)
		for i := range p.Tags {
			key := MakeIndexKey(make([]byte, 0, 20),
				Bytes(Ptr(&p.Level), unsafe.Sizeof(p.Level)),
				[]byte(p.Tags[i])) //every index should append primary key at end
			ret = append(ret, key)
			fmt.Println("idx Tag:", key)
		}
		return ret, nil
	}
	os.Remove("query_test.bdb")
	bdb, err := buntdb.Open("query_test.bdb")
	if err != nil {
		return
	}
	defer bdb.Close()

	kp := KVTParam{
		Bucket:    "Bucket_Book",
		Unmarshal: valueDecode,
		Indexs: []Index{
			{
				&IndexInfo{Name: "Bucket_Book/idx_Type"},
				idx_Type,
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

	k, err := New(book{}, &kp)
	if err != nil {
		t.Errorf("new kvt fail: %s", err)
		return
	}

	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		k.CreateDataBucket(p)
		k.SetSequence(p, 1000)
		k.CreateIndexBuckets(p)
		return nil
	})

	ps := []book{
		book{
			Name:  "Alice",
			Type:  "travel",
			Tags:  []string{"aa", "AA", "xyz"},
			Level: 3,
		},
		book{
			Name:  "Bible",
			Type:  "dictionary",
			Tags:  []string{"bb", "BB", "xyzz"},
			Level: 5,
		},
		book{
			Name:  "Cat",
			Type:  "animal",
			Tags:  []string{"cc", "CC", "xyz"},
			Level: 2,
		},
	}
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
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

	bookEqual := func(p1 book, p2 book) bool {
		return p1.ID == p2.ID && p1.Name == p2.Name && p1.Type == p2.Type && cmpArray(p1.Tags, p2.Tags)
	}
	cmpResult := func(result []any, err error, pm map[uint64]book) {
		if err != nil || len(result) != len(pm) {
			t.Errorf("got query result fail %d %d", len(result), len(pm))
		}
		for i := range result {
			p, _ := result[i].(*book)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]book{ps[2].ID: ps[2]})
		return nil
	})

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

	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]book{ps[1].ID: ps[1]})
		return nil
	})

	rqi = RangeInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string]map[string][]byte{
			"Tags": map[string][]byte{
				"=": []byte("xyz"), //one of tags is "bb"
			},
		},
	}

	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		fmt.Println(err, r, len(r))
		cmpResult(r, err, map[uint64]book{ps[0].ID: ps[0], ps[2].ID: ps[2]})
		return nil
	})

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

	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]book{ps[1].ID: ps[1]})
		return nil
	})

	qi := QueryInfo{
		IndexName: "midx_Level_Tags",
		Where: map[string][]byte{
			//"Type": []byte("book"),
			"Tags": []byte("xyz"),
			//"District": []byte("West ST"),
		},
	}
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]book{ps[0].ID: ps[0], ps[2].ID: ps[2]})
		return nil
	})

	//add a new tag for ps[1]
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]book{ps[0].ID: ps[0], ps[1].ID: ps[1], ps[2].ID: ps[2]})
		return nil
	})

	//change tags of ps[2], remove "xyz"
	bdb.Update(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.View(func(tx *buntdb.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]book{ps[0].ID: ps[0], ps[1].ID: ps[1]})
		return nil
	})
}

func Test_nestBucket(t *testing.T) {

	// generater value
	valueDecode := func(b []byte, obj any) (any, error) {
		r := bytes.NewReader(b)
		dec := gob.NewDecoder(r)
		test := &order2{}
		dec.Decode(test)
		return test, nil
	}

	// generate key of idx_Type_Status
	idx_Type_Status := func(obj interface{}) ([]byte, error) {
		test, _ := obj.(*order2)
		key := MakeIndexKey(make([]byte, 0, 20),
			[]byte(test.Type),
			Bytes(Ptr(&test.Status), unsafe.Sizeof(test.Status))) //every index should append primary key at end
		return key, nil
	}
	os.Remove("query_test.bdb")
	bdb, err := buntdb.Open("query_test.bdb")
	if err != nil {
		return
	}
	defer bdb.Close()

	initkvt := func(mainBucket, idxBucket, idxName string, fields []string) {

		kp := KVTParam{
			Bucket:    mainBucket,
			Unmarshal: valueDecode,
			Indexs: []Index{
				{
					&IndexInfo{
						Name:   idxBucket,
						Fields: fields,
					},
					idx_Type_Status,
				},
			},
		}

		k, err := New(order2{}, &kp)
		if err != nil {
			t.Errorf("new kvt fail: %s", err)
			return
		}

		od := order2{uint64(rand.Int63()), "book", 1}
		bdb.Update(func(tx *buntdb.Tx) error {
			p, _ := NewPoler(tx)
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
		bdb.View(func(tx *buntdb.Tx) error {
			p, _ := NewPoler(tx)
			r, err := k.Query(p, qi)
			if err != nil || len(r) != 1 {
				t.Errorf("query order fail")
				fmt.Println("query order fail:", mainBucket, idxBucket, err, len(r))
				return fmt.Errorf("query order fail %s, %s", mainBucket, idxBucket)
			}
			o := r[0].(*order2)
			if !reflect.DeepEqual(od, *o) {
				t.Errorf("query order fail not equal")
				fmt.Println("query order not equal: ", mainBucket, idxBucket, od, *o)
				return fmt.Errorf("query order fail %s, %s", mainBucket, idxBucket)
			}
			return nil
		})
		bdb.Update(func(tx *buntdb.Tx) error {
			p, _ := NewPoler(tx)
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
