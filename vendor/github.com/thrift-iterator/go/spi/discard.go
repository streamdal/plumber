package spi

func DiscardList(iter Iterator) {
	elemType, size := iter.ReadListHeader()
	for i := 0; i < size; i++ {
		iter.Discard(elemType)
	}
}

func DiscardStruct(iter Iterator) {
	iter.ReadStructHeader()
	for {
		fieldType, _ := iter.ReadStructField()
		if fieldType == 0 {
			return
		}
		iter.Discard(fieldType)
	}
}

func DiscardMap(iter Iterator) {
	keyType, elemType, size := iter.ReadMapHeader()
	for i := 0; i < size; i++ {
		iter.Discard(keyType)
		iter.Discard(elemType)
	}
}
