//go:build bolt
// +build bolt

package kvt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"
)

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
	os.Remove("query_test.bdb")
	bdb, err := bolt.Open("query_test.bdb", 0600, nil)
	if err != nil {
		return
	}
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

	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.Update(func(tx *bolt.Tx) error {
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

	cmpResult := func(result []any, err error, ords map[uint64]Order) {
		//fmt.Println("err:", err, "len result:", len(result))
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
			"Type":     []byte(odInputs[1].Type),
			"Status":   Bytes(Ptr(&odInputs[1].Status), unsafe.Sizeof(odInputs[1].Status)),
			"District": []byte(odInputs[1].District),
		},
	}
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0], odInputs[3].ID: odInputs[3]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[3].ID: odInputs[3]})
		return nil
	})
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

	os.Remove("query_test.bdb")
	bdb, err := bolt.Open("query_test.bdb", 0600, nil)
	if err != nil {
		return
	}
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

	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.Update(func(tx *bolt.Tx) error {
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[0].ID: odInputs[0]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[4].ID: odInputs[4]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1], odInputs[4].ID: odInputs[4]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2], odInputs[1].ID: odInputs[1]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[2].ID: odInputs[2]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[1].ID: odInputs[1], odInputs[2].ID: odInputs[2], odInputs[3].ID: odInputs[3], odInputs[4].ID: odInputs[4]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Order{odInputs[3].ID: odInputs[3]})
		return nil
	})
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
	os.Remove("query_test.bdb")
	bdb, err := bolt.Open("query_test.bdb", 0600, nil)
	if err != nil {
		return
	}
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

	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]People{ps[2].ID: ps[2]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]People{ps[1].ID: ps[1]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]People{ps[0].ID: ps[0], ps[2].ID: ps[2]})
		return nil
	})
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
	os.Remove("query_test.bdb")
	bdb, err := bolt.Open("query_test.bdb", 0600, nil)
	if err != nil {
		return
	}
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

	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
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
	bdb.Update(func(tx *bolt.Tx) error {
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

	bookEqual := func(p1 Book, p2 Book) bool {
		return p1.ID == p2.ID && p1.Name == p2.Name && p1.Type == p2.Type && cmpArray(p1.Tags, p2.Tags)
	}
	cmpResult := func(result []any, err error, pm map[uint64]Book) {
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Book{ps[2].ID: ps[2]})
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

	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Book{ps[1].ID: ps[1]})
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

	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		fmt.Println(err, r, len(r))
		cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[2].ID: ps[2]})
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

	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.RangeQuery(p, rqi)
		cmpResult(r, err, map[uint64]Book{ps[1].ID: ps[1]})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[2].ID: ps[2]})
		return nil
	})

	//add a new tag for ps[1]
	bdb.Update(func(tx *bolt.Tx) error {
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[1].ID: ps[1], ps[2].ID: ps[2]})
		return nil
	})

	//change tags of ps[2], remove "xyz"
	bdb.Update(func(tx *bolt.Tx) error {
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := NewPoler(tx)
		r, err := k.Query(p, qi)
		cmpResult(r, err, map[uint64]Book{ps[0].ID: ps[0], ps[1].ID: ps[1]})
		return nil
	})
}
