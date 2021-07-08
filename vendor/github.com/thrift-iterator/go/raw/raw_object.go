package raw

import "github.com/thrift-iterator/go/protocol"

type StructField struct {
	Buffer []byte
	Type protocol.TType
}

type Struct map[protocol.FieldId]StructField

type List struct {
	ElementType protocol.TType
	Elements [][]byte
}

type MapEntry struct {
	Key []byte
	Element []byte
}

type Map struct {
	KeyType protocol.TType
	ElementType protocol.TType
	Entries map[interface{}]MapEntry
}