package kvt

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

type IndexFunc = func(any) ([]byte, error)
type EncodeFunc = func(any) ([]byte, error)
type DecodeFunc = func([]byte, KVer) (KVer, error)
type CompareFunc = func(d, v []byte) bool
type FilterFunc = func(k []byte) bool
type MIndexFunc = func(any) ([][]byte, error) //a index func return multi value

// 2 index type index, mindex
const IDXPrefix = "idx_"   //index name prefix
const MIDXPrefix = "midx_" //mindex return multi index value from one field, it support  slice/array field

const defaultPathJoiner = '/'
const defaultIDXJoiner = '_'
const defaultKeyJoiner = ':' //field1:field2,  escape ':' with "`"
const defaultKeyEscaper = '`'
const sequenceName = "__sequence__"

// invalid index format, maybe contain invalid charactor,  should like that IDX(field1_field2_...)
const errFormatInvalid = "index format invalid: [%s]"

// current index has defined before, or repeat index
const errIndexConflict = "index name conflict: [%s], primary key should be unique"

// register index failed
const errIndexFieldMismatch = "index field match failed: [%s], please confirm index field exists"

// index doesn't exist
const errIndexNotExist = "index not found or doesn't exist: [%s]"

// index key generator not found
//const errIndexFuncNotFound = "index [%s] not found generate key function"

const errBucketOpenFailed = "bucket [%s] not found or open failed"

const errCompareOperatorInvalid = "compare operator [%s] is invalid"

const errNewPolerFailed = "new poler failed, invalid db handler"

const errIndexFunctionNotFound = "index function not found"

const errDataNotFound = "data not found"

type KVer interface {
	Key() ([]byte, error)
	Value() ([]byte, error)
	Index(string) ([]byte, error)
}

type KVT struct {
	bucket    string //bucket or table name
	path      string //its parent path
	unmarshal DecodeFunc
	indexs    map[string]*IndexInfo //(indexName, *IDX)
	mindexs   map[string]MIndex
}

type IndexInfo struct {
	Name   string   //index name like "idx_field1_field2"
	Fields []string //["field1", "field2"...]
	path   string   //full paraent path to index, eg   "root/to/Bucket"
	offset int      //some kv db doesn't support bucket, so add bucket name in the key, it's a bucket prefix offset
}

type MIndex struct {
	*IndexInfo
	Key MIndexFunc //generate index multi key, value is the pk
}

type KVTParam struct {
	Bucket    string      //bucket (with its paraent if exists), eg: "root/path/to/your/Bucket"
	Unmarshal DecodeFunc  //unmarshal value bytes to a object
	Indexs    []IndexInfo //generate idx bucket's key
	MIndexs   []MIndex
}

func parse(obj any) map[string]struct{} {
	fields := reflect.TypeOf(obj)
	num := fields.NumField()

	fieldNames := make(map[string]struct{})

	for i := range num {
		field := fields.Field(i)
		fieldNames[field.Name] = struct{}{}
	}

	return fieldNames
}

// user give us the full path name, need split into []
func splitPath(fullPath string) (string, []string, error) {
	name := strings.TrimSpace(fullPath)
	names := strings.Split(name, string(defaultPathJoiner))
	switch len(names) {
	case 0:
		return "", []string{}, fmt.Errorf(errFormatInvalid, fullPath)
	case 1:
		return names[0], []string{}, nil
	default:
		return names[len(names)-1], names[:len(names)-1], nil
	}
}

func makeIndexInfo(name string, fields, p []string) *IndexInfo {
	idx := &IndexInfo{
		Name: name,
	}
	idx.Fields = append(idx.Fields, fields...)
	idx.path = strings.Join(p, string(defaultPathJoiner))

	if len(idx.Fields) == 0 {
		fields := strings.Split(name, string(defaultIDXJoiner))
		if len(fields) > 0 {
			switch fields[0] + string(defaultIDXJoiner) {
			case IDXPrefix, MIDXPrefix:
				idx.Fields = append(idx.Fields, fields[1:]...)
			}
		}
	}

	return idx
}

// user give us the full path name, need split into []
func (kvt *KVT) saveIndexs(kp *KVTParam) error {
	//the main bucket
	bucket, mainPath, err := splitPath(kp.Bucket)
	if err != nil {
		return err
	}
	kvt.bucket = bucket
	kvt.path = kp.Bucket

	//indexs
	kvt.indexs = make(map[string]*IndexInfo, len(kp.Indexs))
	for i := range kp.Indexs {
		name, path, err := splitPath(kp.Indexs[i].Name)
		if err != nil || len(name) == 0 {
			return err
		}

		if _, ok := kvt.indexs[name]; ok {
			return fmt.Errorf(errIndexConflict, name)
		}
		var p []string
		//here we add prefix main bucket name as idx path
		//for a idx like "idx_Type" is very possible conflict
		//with another objects's "idx_Type"
		if (len(path) > 0 && path[0] == kvt.bucket) ||
			len(path) == 0 && len(kp.Indexs[i].Fields) == 0 {
			p = append(p, mainPath...)
			if len(path) == 0 {
				p = append(p, kvt.bucket)
			}
		}
		p = append(p, path...)
		p = append(p, name)
		kvt.indexs[name] = makeIndexInfo(name, kp.Indexs[i].Fields, p)
	}

	//mindexs
	kvt.mindexs = make(map[string]MIndex, len(kp.MIndexs))
	for i := range kp.MIndexs {
		name, path, err := splitPath(kp.MIndexs[i].Name)
		if err != nil {
			return err
		}
		if _, ok := kvt.mindexs[name]; ok {
			return fmt.Errorf(errIndexConflict, name)
		}
		var p []string
		if len(path) > 0 && path[0] == kvt.bucket { //index nested in data bucket
			p = append(p, mainPath...)
		}
		p = append(p, path...) //index path
		p = append(p, name)    //index bucket
		kvt.mindexs[name] = MIndex{makeIndexInfo(name, kp.MIndexs[i].Fields, p), kp.MIndexs[i].Key}
	}

	return nil
}

// if without fields need split fields from index name, excluding the inde prefix
func checkIndexFields(index *IndexInfo, allFields map[string]struct{}) error {
	if len(index.Fields) == 0 {
		return fmt.Errorf(errIndexFieldMismatch, index.Name)
	}
	for _, v := range index.Fields {
		if _, ok := allFields[v]; !ok {
			return fmt.Errorf(errIndexFieldMismatch, index.Name)
		}
	}
	return nil
}

func (kvt *KVT) checkIndexsFields(allFields map[string]struct{}) error {
	//index
	for i := range kvt.indexs {
		if err := checkIndexFields(kvt.indexs[i], allFields); err != nil {
			return err
		}
	}

	//MIndex
	for i := range kvt.mindexs {
		if err := checkIndexFields(kvt.mindexs[i].IndexInfo, allFields); err != nil {
			return err
		}
	}

	return nil
}

func New(obj any, kp *KVTParam) (kvt *KVT, err error) {
	kvt = &KVT{
		bucket:    kp.Bucket,
		unmarshal: kp.Unmarshal,
	}

	if err := kvt.saveIndexs(kp); err != nil {
		return nil, err
	}

	fields := parse(obj)
	if err := kvt.checkIndexsFields(fields); err != nil {
		return nil, err
	}
	return kvt, nil
}

// create main data bucket only
func (kvt *KVT) CreateDataBucket(db Poler) (err error) {
	_, _, err = db.CreateBucket(kvt.path)
	if err != nil {
		return err
	}
	//kvt.path = string(prefix) //save prefix for Put/Delete
	//v.offset = len(prefix)              //save prefix for query
	return err
}

// delete main data bucket, DANGEROUSE, you will lost all you data
func (kvt *KVT) DeleteDataBucket(db Poler) (err error) {

	return db.DeleteBucket(kvt.path)
}

// create the index buckets
func (kvt *KVT) CreateIndexBuckets(db Poler) (err error) {
	for _, v := range kvt.indexs {
		_, offset, err := db.CreateBucket(v.path)
		if err != nil {
			return err
		}
		//v.path = string(prefix) //save prefix for Put/Delete
		v.offset = offset //save prefix for query

	}
	for _, v := range kvt.mindexs {
		_, offset, err := db.CreateBucket(v.path)
		if err != nil {
			return err
		}
		//v.path = string(prefix)
		v.offset = offset
	}
	return nil
}

// delete all the index buckets
func (kvt *KVT) DeleteIndexBuckets(db Poler) error {

	for _, v := range kvt.indexs {
		if err := db.DeleteBucket(v.path); err != nil {
			return err
		}
	}

	for _, v := range kvt.mindexs {
		if err := db.DeleteBucket(v.path); err != nil {
			return err
		}
	}

	return nil
}

func (kvt *KVT) Put(db Poler, obj KVer) error {
	key, _ := obj.Key()
	value, _ := obj.Value()

	old, err := db.Get(kvt.path, key)
	if err != nil {
		return err
	}

	if len(old) > 0 { // update the exist INDEX
		oldObj, err := kvt.unmarshal(old, nil)
		if err != nil {
			return err
		}
		for i := range kvt.indexs {
			kold, _ := oldObj.Index(kvt.indexs[i].Name)
			knew, _ := obj.Index(kvt.indexs[i].Name)
			if bytes.Equal(kold, knew) {
				continue
			}
			kold = AppendLastKey(kold, key)
			if err = db.Delete(kvt.indexs[i].path, kold); err != nil {
				return err
			}
			knew = AppendLastKey(knew, key) //index key should append primary key, to make sure it unique
			if err := db.Put(kvt.indexs[i].path, knew, key); err != nil {
				return err
			}
		}
		//for mindex, we delete olds
		kvt.deleteMIndex(db, oldObj, key)
	} else { //insert new index, and point to the primary key

		for i := range kvt.indexs {
			ik, _ := obj.Index(kvt.indexs[i].Name)
			ik = AppendLastKey(ik, key) //index key should append primary key, to make sure it unique
			if err := db.Put(kvt.indexs[i].path, ik, key); err != nil {
				return err
			}
		}
	}
	//insert new MIndex
	for i := range kvt.mindexs {
		iks, _ := kvt.mindexs[i].Key(obj) //index key
		for j := range iks {
			ik := AppendLastKey(iks[j], key) //index key should append primary key, to make sure it unique
			if err := db.Put(kvt.mindexs[i].path, ik, key); err != nil {
				return err
			}
		}
	}

	return db.Put(kvt.path, key, value)
}

func (kvt *KVT) deleteMIndex(db Poler, obj any, pk []byte) error {
	for i := range kvt.mindexs {
		kolds, _ := kvt.mindexs[i].Key(obj)
		for j := range kolds {
			kold := AppendLastKey(kolds[j], pk)
			if err := db.Delete(kvt.mindexs[i].path, kold); err != nil {
				return err
			}
		}
	}
	return nil
}

func (kvt *KVT) Delete(db Poler, obj KVer) error {
	key, _ := obj.Key()
	old, err := db.Get(kvt.path, key)
	if err != nil || len(old) == 0 {
		return err
	}

	oldObj, err := kvt.unmarshal(old, nil)
	if err != nil {
		return err
	}
	for i := range kvt.indexs {
		kold, _ := oldObj.Index(kvt.indexs[i].Name)
		kold = AppendLastKey(kold, key)
		if err := db.Delete(kvt.indexs[i].path, kold); err != nil {
			return err
		}
	}
	if err := kvt.deleteMIndex(db, oldObj, key); err != nil {
		return err
	}

	if err = db.Delete(kvt.path, key); err != nil {
		return err
	}
	return nil
}

// query the current sequence of the table, read tx, will not change it
func (kvt *KVT) Sequence(db Poler) (uint64, error) {
	return db.Sequence(kvt.path)
}

// query the next sequence of the table, you should fill it into the primary key, it will Inc it every query
func (kvt *KVT) NextSequence(db Poler) (uint64, error) {
	return db.NextSequence(kvt.path)
}

// update the sequence directly
func (kvt *KVT) SetSequence(db Poler, seq uint64) error {
	return db.SetSequence(kvt.path, seq)
}
