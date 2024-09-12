package kvt

import (
	"bytes"
	"encoding/gob"
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
