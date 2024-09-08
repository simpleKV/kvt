KVT is a pure GO index manager lib for indexing key-value based objects

define index with some funcs and a struct, KVT will auto maintain the index when you CRUD it

the most important is you can query with KVT by the index you define, just as sqlite or other database system do for you

KVT is NOT a KV system, it's a index manager only, its aim is to integrate with all other database based KV 

Now support KV lists:  BoltDB, BuntDB, Redis...


Features
========

- support union index, one field or multi fields
- index support full compare query(=,<, >...), range query
- index support all data type(int, string, time...) 
- support multi indexs for one struct
- support partial index query(you can omit some index fields)
- support slice index(contain query with midx)
- KVT self depends on reflect package few, only a check when init, but maybe your APP code need depend reflect when marshal/unmarshal
- support many kv DB, it will very easy to add a new kv driver, now tested BoltDB/BuntDB/Redis 
- support spec data/index bucket path
- most import, very easy to use and integrate with other code


flow
========
![kvt work flow](kvt.png)


install
========
```
go get github.com/simpleKV/kvt 
```
then build you project with One driver build tag: boltdb/buntdb
```
go build -tags boltdb   //build with boltdb, if you want use BoltDB
go build -tags buntdb   //build with buntdb  if you want use BuntDB
go build -tags redis    //build with redis  if you want use Redis
```

sample
========
https://github.com/simpleKV/kvtsample

