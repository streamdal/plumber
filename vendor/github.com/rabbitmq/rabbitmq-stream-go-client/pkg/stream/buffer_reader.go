package stream

import (
	"bufio"
	"encoding/binary"
	"io"
)

func readUShort(readerStream io.Reader) uint16 {
	var res uint16
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func readShort(readerStream io.Reader) int16 {
	var res int16
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func readUInt(readerStream io.Reader) (uint32, error) {
	var res uint32
	err := binary.Read(readerStream, binary.BigEndian, &res)
	return res, err
}

func peekByte(readerStream *bufio.Reader) (uint8, error) {
	res, err := readerStream.Peek(1)
	if err != nil {
		return 0, err
	}
	return res[0], nil
}

func readInt64(readerStream io.Reader) int64 {
	var res int64
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func readByte(readerStream io.Reader) uint8 {
	var res uint8
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func readByteError(readerStream io.Reader) (uint8, error) {
	var res uint8
	err := binary.Read(readerStream, binary.BigEndian, &res)
	return res, err
}

func readString(readerStream io.Reader) string {
	lenString := readUShort(readerStream)
	buff := make([]byte, lenString)
	_ = binary.Read(readerStream, binary.BigEndian, &buff)
	return string(buff)
}

func readUint8Array(readerStream io.Reader, size uint32) []uint8 {
	var res = make([]uint8, size)
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}
