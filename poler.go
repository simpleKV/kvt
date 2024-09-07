package kvt

type Poler interface {
	CreateBucket(path []string) ([]byte, int, error)
	DeleteBucket(path []string) error

	Put(path string, k []byte, v []byte) error
	Get(path string, k []byte) ([]byte, error)
	Delete(path string, k []byte) error
	//Query
	Query(path string, prefix []byte, filter FilterFunc) ([]KVPair, error)

	//sequence api
	Sequence(path string) (uint64, error)
	NextSequence(path string) (uint64, error)
	SetSequence(path string, seq uint64) error
}
