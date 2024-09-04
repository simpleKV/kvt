package kvt

type Poler interface {
	CreateBucket(path []string) ([]byte, error)
	DeleteBucket(path []string) error

	Put(k []byte, v []byte, path []string) error
	Get(k []byte, path []string) ([]byte, error)
	Delete(k []byte, path []string) error
	//Query
	Query(prefix []byte, filter FilterFunc, path []string) ([]KVPair, error)

	//sequence api
	Sequence(path []string) (uint64, error)
	NextSequence(path []string) (uint64, error)
	SetSequence(path []string, seq uint64) error
}
