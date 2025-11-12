package datastore

type DataType string

const (
	String DataType = "string"
	Binary DataType = "binary"
)

type MetaData struct {
	Meta map[string]string
}

type Data struct {
	data any
	MetaData
	Type DataType
}

func (d *Data) PutValue(value any, metaData MetaData) {}

func (d *Data) GetValue() any {
	return d.data
}

func (d *Data) GetMetaData() MetaData {
	return MetaData{}
}

type Record struct {
	ID    string
	Key   string
	Value Data
}

type Store interface {
	Read(key string) ([]byte, error)
	Write(key string, record Record) error
	Delete(key string) error
}

type DataStore struct {
	store Store
}
