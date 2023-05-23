package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

func writeLong(inputBuff *bytes.Buffer, value int64) {
	writeULong(inputBuff, uint64(value))
}

func writeULong(inputBuff *bytes.Buffer, value uint64) {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	inputBuff.Write(buff)
}

func writeBLong(inputBuff *bufio.Writer, value int64) {
	writeBULong(inputBuff, uint64(value))
}
func writeBULong(inputBuff *bufio.Writer, value uint64) {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	inputBuff.Write(buff)
}

func writeShort(inputBuff *bytes.Buffer, value int16) {
	writeUShort(inputBuff, uint16(value))
}

func writeUShort(inputBuff *bytes.Buffer, value uint16) {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	inputBuff.Write(buff)
}

func writeBShort(inputBuff *bufio.Writer, value int16) {
	writeBUShort(inputBuff, uint16(value))
}
func writeBUShort(inputBuff *bufio.Writer, value uint16) {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	inputBuff.Write(buff)
}

func writeByte(inputBuff *bytes.Buffer, value byte) {
	var buff = make([]byte, 1)
	buff[0] = value
	inputBuff.Write(buff)
}

func writeBByte(inputBuff *bufio.Writer, value byte) {
	var buff = make([]byte, 1)
	buff[0] = value
	inputBuff.Write(buff)
}

func writeInt(inputBuff *bytes.Buffer, value int) {
	writeUInt(inputBuff, uint32(value))
}
func writeUInt(inputBuff *bytes.Buffer, value uint32) {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	inputBuff.Write(buff)
}

func writeBInt(inputBuff *bufio.Writer, value int) {
	writeBUInt(inputBuff, uint32(value))
}

func writeBUInt(inputBuff *bufio.Writer, value uint32) {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	inputBuff.Write(buff)
}

func bytesFromInt(value uint32) []byte {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	return buff
}
func writeString(inputBuff *bytes.Buffer, value string) {
	writeUShort(inputBuff, uint16(len(value)))
	inputBuff.Write([]byte(value))
}

func writeBytes(inputBuff *bytes.Buffer, value []byte) {
	inputBuff.Write(value)
}

// writeProtocolHeader  protocol utils functions
func writeProtocolHeader(inputBuff *bytes.Buffer,
	length int, command int16,
	correlationId ...int) {

	writeInt(inputBuff, length)
	writeShort(inputBuff, command)
	writeShort(inputBuff, version1)
	if len(correlationId) > 0 {
		writeInt(inputBuff, correlationId[0])
	}

}

func writeBProtocolHeader(inputBuff *bufio.Writer,
	length int, command int16,
	correlationId ...int) {

	writeBInt(inputBuff, length)
	writeBShort(inputBuff, command)
	writeBShort(inputBuff, version1)
	if len(correlationId) > 0 {
		writeBInt(inputBuff, correlationId[0])
	}

}
