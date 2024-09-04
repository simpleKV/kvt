package kvt

import (
	"bytes"
	"fmt"
	"strings"
)

var cmpFunctionDict map[string]CompareFunc = map[string]CompareFunc{
	"=":  cmpEqual,
	"==": cmpEqual,
	"<":  cmpLess,
	">":  cmpLarge,
	"<=": cmpLessEqual,
	">=": cmpLargeEqual,
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// now support equal only, it's for user param
type QueryInfo struct {
	IndexName string
	Where     map[string][]byte //(fieldName, value)
}

type RangeInfo struct {
	IndexName string
	Where     map[string]map[string][]byte //(fieldName, value)
}

// check if data == v
func cmpEqual(data, v []byte) bool {
	return bytes.Equal(data, v)
}

// check if data < v
func cmpLess(data, v []byte) bool {
	return bytes.Compare(data, v) < 0
}

// data <= v
func cmpLessEqual(data, v []byte) bool {
	return !cmpLarge(data, v)
}

// check if data > v
func cmpLarge(data, v []byte) bool {
	return bytes.Compare(data, v) > 0
}

func cmpLargeEqual(data, v []byte) bool {
	return !cmpLess(data, v)
}

type cmpValueInfo struct {
	value []byte
	cmp   func(d, v []byte) bool
}

type cmpQueryInfo struct {
	*IndexInfo
	Where map[string][]cmpValueInfo //(fieldName, value)
}

func (kvt *KVT) assignFieldsName(partial cmpQueryInfo, fieldValues [][]byte) (result map[string][]byte) {

	idx := partial.IndexInfo
	result = make(map[string][]byte, len(idx.Fields))

	for i := range idx.Fields {
		result[idx.Fields[i]] = fieldValues[i]
	}
	return result
}

// return true if match with queryinfo or empty queryinfo found, otherwise false
func (kvt *KVT) comparePartialQueryInfo(partial cmpQueryInfo, index []byte) bool {
	if len(partial.Where) == 0 {
		return true
	}

	//need remove the bucket prefix, for the db which doesn't support
	keys := SplitIndexKey(index[partial.IndexInfo.offset:])
	//index keys will append a PK at end, so it should large than index's fields
	if len(keys) != len(partial.IndexInfo.Fields)+1 {
		return false
	}
	result := kvt.assignFieldsName(partial, keys)

	for k, v := range partial.Where {
		for i := range v {
			if !v[i].cmp(result[k], v[i].value) {
				return false
			}
		}
	}

	return true
}

func (kvt *KVT) getIndexInfo(name string) (index *IndexInfo, err error) {
	switch {
	case strings.HasPrefix(name, IDXPrefix):
		v, ok := kvt.indexs[name]
		if !ok {
			return nil, fmt.Errorf(errIndexNotExist, name)
		}
		return v.IndexInfo, nil
	case strings.HasPrefix(name, MIDXPrefix):
		v, ok := kvt.mindexs[name]
		if !ok {
			return nil, fmt.Errorf(errIndexNotExist, name)
		}
		return v.IndexInfo, nil
		//case strings.HasPrefix(name, PKPrefix): //how about query with pk ??
	}
	return nil, fmt.Errorf(errIndexNotExist, name)
}

// query by index, and support fields range query
func (kvt *KVT) RangeQuery(db Poler, rangeInfo RangeInfo) (result []any, err error) {

	index, err := kvt.getIndexInfo(rangeInfo.IndexName)
	if err != nil || index == nil {
		return nil, fmt.Errorf(errIndexNotExist, rangeInfo.IndexName)
	}

	//save all the equal query field with it's byte[] value
	var equals map[string][]byte = make(map[string][]byte, len(index.Fields))

	//check if fields info exists in index
	for i := range rangeInfo.Where {
		found := false
		for j := range index.Fields {
			if i == index.Fields[j] {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf(errIndexFieldMismatch, i)
		}
		equals[i] = []byte{}
		for j, v := range rangeInfo.Where[i] {
			cmpMode := strings.TrimSpace(j)
			_, ok := cmpFunctionDict[cmpMode]
			if !ok {
				return nil, fmt.Errorf(errCompareOperatorInvalid, j)
			}
			if (cmpMode == "=" || cmpMode == "==") && len(v) > 0 {
				equals[i] = v
			}
		}
	}

	prefix := make([]byte, 0)

	i := 0
	partialQueryInfo := cmpQueryInfo{
		IndexInfo: index,
		Where:     make(map[string][]cmpValueInfo, len(index.Fields)),
	}
	partial := false
	for i = range index.Fields {
		name := index.Fields[i]
		found, ok := rangeInfo.Where[name]
		//if not found, then all the later fields are partial query
		if !ok {
			partial = true
			continue
		}
		//if meet a range query, then partial query begin
		if len(equals[name]) == 0 {
			partial = true
		}

		if partial {
			for j := range found {
				//here we have checked all the compare opereator is valid before
				partialQueryInfo.Where[name] = append(partialQueryInfo.Where[name], cmpValueInfo{found[j], cmpFunctionDict[strings.TrimSpace(j)]})
			}
		} else {
			prefix = MakeIndexKey(prefix, equals[name])
		}
	}
	filter := func(k []byte) bool {
		return true
	}
	if partial {
		filter = func(k []byte) bool {
			return kvt.comparePartialQueryInfo(partialQueryInfo, k)
		}
	}

	pks, err := db.Query(prefix, filter, index.path) //query (key, pk) pair
	if err != nil {
		return result, err
	}

	for i := range pks {
		v, err := db.Get(pks[i].Value, kvt.path)

		if err != nil {
			return result, err
		}
		obj, _ := kvt.unmarshal(v, nil)
		result = append(result, obj)
	}

	return result, nil
}

// simple query by the index, keys is the pairs of (fieldName, value []byte)
// support fields equal only
func (kvt *KVT) Query(db Poler, info QueryInfo) (result []any, err error) {

	//convert to RangeInfo
	rangeInfo := RangeInfo{
		IndexName: info.IndexName,
		Where:     make(map[string]map[string][]byte, len(info.Where)),
	}
	for k, v := range info.Where {
		rangeInfo.Where[k] = map[string][]byte{"=": v}
	}

	return kvt.RangeQuery(db, rangeInfo)
}

// get a full obj with its pk only
func (kvt *KVT) Get(db Poler, obj any, dst any) (any, error) {

	key, _ := kvt.pk.Key(obj)
	oldByte, err := db.Get(key, kvt.path)

	if err == nil || len(oldByte) == 0 {
		return dst, nil
	}
	return kvt.unmarshal(oldByte, dst)
}

// get all objs with prefixs/key bytes
func (kvt *KVT) Gets(db Poler, prefix []byte) (result []any, err error) {

	pks, err := db.Query(prefix, func([]byte) bool { return true }, kvt.path) //query (key, pk) pair
	if err != nil {
		return result, err
	}

	for i := range pks {
		obj, _ := kvt.unmarshal(pks[i].Value, nil)
		result = append(result, obj)
	}

	return result, nil
}
