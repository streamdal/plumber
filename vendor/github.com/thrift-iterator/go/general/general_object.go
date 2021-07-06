package general

import "github.com/thrift-iterator/go/protocol"

type Object interface {
	Get(path ...interface{}) interface{}
}

type List []interface{}

func (obj List) Get(path ...interface{}) interface{} {
	if len(path) == 0 {
		return obj
	}
	elem := obj[path[0].(int)]
	if len(path) == 1 {
		return elem
	}
	return elem.(Object).Get(path[1:]...)
}

type Map map[interface{}]interface{}

func (obj Map) Get(path ...interface{}) interface{} {
	if len(path) == 0 {
		return obj
	}
	elem := obj[path[0]]
	if len(path) == 1 {
		return elem
	}
	return elem.(Object).Get(path[1:]...)
}

type Struct map[protocol.FieldId]interface{}

func (obj Struct) Get(path ...interface{}) interface{} {
	if len(path) == 0 {
		return obj
	}
	elem := obj[path[0].(protocol.FieldId)]
	if len(path) == 1 {
		return elem
	}
	return elem.(Object).Get(path[1:]...)
}

type Message struct {
	protocol.MessageHeader
	Arguments Struct
}
