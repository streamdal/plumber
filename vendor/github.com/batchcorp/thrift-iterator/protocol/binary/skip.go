package binary

import "github.com/batchcorp/thrift-iterator/protocol"

func (iter *Iterator) skip(skipper func(), space []byte) []byte {
	var tmp []byte
	iter.skipped = make([]byte, 0, 8)
	skipper()
	tmp, iter.skipped = iter.skipped, nil
	if iter.Error() != nil {
		return nil
	}
	if len(space) > 0 {
		return append(space, tmp...)
	}
	return tmp
}

func (iter *Iterator) Skip(ttype protocol.TType, space []byte) []byte {
	return iter.skip(func() { iter.Discard(ttype) }, space)
}

func (iter *Iterator) SkipMessageHeader(space []byte) []byte {
	return iter.skip(func() { iter.ReadMessageHeader() }, space)
}

func (iter *Iterator) SkipStruct(space []byte) []byte {
	return iter.skip(func() { iter.Discard(protocol.TypeStruct) }, space)
}

func (iter *Iterator) SkipList(space []byte) []byte {
	return iter.skip(func() { iter.Discard(protocol.TypeList) }, space)
}

func (iter *Iterator) SkipMap(space []byte) []byte {
	return iter.skip(func() { iter.Discard(protocol.TypeMap) }, space)
}

func (iter *Iterator) SkipBinary(space []byte) []byte {
	tmp := iter.ReadBinary()
	if iter.Error() != nil {
		return nil
	}
	if len(space) > 0 {
		return append(space, tmp...)
	}
	return tmp
}
