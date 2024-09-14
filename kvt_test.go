package kvt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
	"unsafe"
)

type order struct {
	ID       uint64
	Type     string
	Status   uint16
	Name     string
	District string
	Num      int
}

func (obj *order) Key() ([]byte, error) {
	return Bytes(Ptr(&obj.ID), unsafe.Sizeof(obj.ID)), nil
}

func (obj *order) Value() ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	enc.Encode(obj)

	return network.Bytes(), nil
}

// produce a primary key(pk) from a Order object,
// save  in the main bucket like that (pk(return by order_pk_ID),  value(return by order_valueEncode))
func (this *order) Index(name string) ([]byte, error) {
	switch name {
	case "idx_Type_Status_District":
		return this.idx_Type_Status_District()
	}
	return nil, fmt.Errorf("Index not found")
}

func (this *order) idx_Type_Status_District() ([]byte, error) {
	key := MakeIndexKey(make([]byte, 0, 20),
		[]byte(this.Type),
		Bytes(Ptr(&this.Status), unsafe.Sizeof(this.Status)),
		[]byte(this.District)) //every index should append primary key at end
	return key, nil
}

type people struct {
	ID    uint64
	Name  string
	Birth time.Time
}

func (obj *people) Key() ([]byte, error) {
	return Bytes(Ptr(&obj.ID), unsafe.Sizeof(obj.ID)), nil
}

func (obj *people) Value() ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)

	enc.Encode(obj)

	return network.Bytes(), nil
}

func (obj *people) Index(name string) ([]byte, error) {
	switch name {
	case "idx_Birth":
		return obj.idx_Birth()
	default:
		return nil, fmt.Errorf("index [%s] not found", name)
	}
}

// generate key of idx_Type_Status
func (obj *people) idx_Birth() ([]byte, error) {
	key := MakeIndexKey(make([]byte, 0, 20),
		[]byte(obj.Birth.Format(time.RFC3339))) //every index should append primary key at end
	return key, nil
}

type book struct {
	ID    uint64
	Name  string
	Type  string
	Tags  []string
	Level int
}

func (obj *book) Key() ([]byte, error) {
	return Bytes(Ptr(&obj.ID), unsafe.Sizeof(obj.ID)), nil
}

func (obj *book) Value() ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	enc.Encode(obj)

	return network.Bytes(), nil
}

func (obj *book) Index(name string) ([]byte, error) {
	switch name {
	case "idx_Type":
		return obj.idx_Type()
	default:
		return nil, fmt.Errorf("index [%s] not found", name)
	}
}

// generate key of idx_Type
func (obj *book) idx_Type() ([]byte, error) {
	key := MakeIndexKey(make([]byte, 0, 20),
		[]byte(obj.Type)) //every index should append primary key at end
	return key, nil
}

type order2 struct {
	ID     uint64
	Type   string
	Status uint16
}

func (obj *order2) Key() ([]byte, error) {
	return Bytes(Ptr(&obj.ID), unsafe.Sizeof(obj.ID)), nil
}
func (obj *order2) Value() ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	enc.Encode(obj)

	return network.Bytes(), nil
}

// order2 is for test bucket path, only 1 index idx_Type_Status
func (obj *order2) Index(name string) ([]byte, error) {
	fmt.Println("order2 Index ", name)
	return obj.idx_Type_Status()
}

// generate key of idx_Type_Status
func (obj *order2) idx_Type_Status() ([]byte, error) {
	key := MakeIndexKey(make([]byte, 0, 20),
		[]byte(obj.Type),
		Bytes(Ptr(&obj.Status), unsafe.Sizeof(obj.Status))) //every index should append primary key at end
	return key, nil
}
