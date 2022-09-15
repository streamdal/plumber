package stream

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"io"
	"io/ioutil"
)

const (
	None   = byte(0)
	GZIP   = byte(1)
	SNAPPY = byte(2)
	LZ4    = byte(3)
	ZSTD   = byte(4)
)

type Compression struct {
	enabled bool
	value   byte
}

func (compression Compression) String() string {
	return fmt.Sprintf("value %d (enable: %t)", compression.value,
		compression.enabled)
}

func (compression Compression) None() Compression {
	return Compression{value: None, enabled: true}
}

func (compression Compression) Gzip() Compression {
	return Compression{value: GZIP, enabled: true}
}

func (compression Compression) Snappy() Compression {
	return Compression{value: SNAPPY, enabled: true}
}

func (compression Compression) Zstd() Compression {
	return Compression{value: ZSTD, enabled: true}
}

func (compression Compression) Lz4() Compression {
	return Compression{value: LZ4, enabled: true}
}

type subEntry struct {
	messages     []messageSequence
	publishingId int64 // need to store the publishingId useful in case of aggregation

	unCompressedSize int
	sizeInBytes      int
	dataInBytes      []byte
}
type subEntries struct {
	items            []*subEntry
	totalSizeInBytes int
}

type iCompress interface {
	Compress(subEntries *subEntries)
	UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader
}

func compressByValue(value byte) iCompress {

	switch value {
	case GZIP:
		return compressGZIP{}
	case SNAPPY:
		return compressSnappy{}
	case LZ4:
		return compressLZ4{}
	case ZSTD:
		return compressZSTD{}
	}

	return compressNONE{}
}

type compressNONE struct {
}

func (es compressNONE) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		for _, msg := range entry.messages {
			writeInt(&tmp, len(msg.messageBytes))
			tmp.Write(msg.messageBytes)
		}
		entry.dataInBytes = tmp.Bytes()
		entry.sizeInBytes += len(entry.dataInBytes)
		subEntries.totalSizeInBytes += len(entry.dataInBytes)
	}
}

func (es compressNONE) UnCompress(source *bufio.Reader, _, _ uint32) *bufio.Reader {
	return source
}

type compressGZIP struct {
}

func (es compressGZIP) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := gzip.NewWriter(&tmp)
		for _, msg := range entry.messages {
			size := len(msg.messageBytes)
			//w.Write(bytesFromInt((uint32(size) >> 24) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 16) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 8) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 0) & 0xFF))
			w.Write(bytesFromInt(uint32(size)))
			w.Write(msg.messageBytes)
		}
		w.Flush()
		w.Close()
		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}
}

func (es compressGZIP) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader {

	var zipperBuffer = make([]byte, dataSize)
	/// empty
	_, err := io.ReadFull(source, zipperBuffer)
	/// array of compress data

	if err != nil {
		logs.LogError("GZIP Error during reading buffer %s", err)
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(zipperBuffer))

	if err != nil {
		logs.LogError("Error creating GZIP NewReader  %s", err)
	}
	defer reader.Close()
	/// headers ---> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := ioutil.ReadAll(reader)
	if err != nil {
		logs.LogError("Error during reading buffer %s", err)
	}
	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		panic("uncompressedDataSize != count")
	}

	/// headers ---> payload --> headers --> payload (compressed) --> uncompressed payload

	return bufio.NewReader(bytes.NewReader(uncompressedReader))
}

type compressSnappy struct {
}

func (es compressSnappy) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := snappy.NewBufferedWriter(&tmp)
		for _, msg := range entry.messages {
			size := len(msg.messageBytes)
			if _, err := w.Write(bytesFromInt(uint32(size))); err != nil {
				logs.LogError("Error compressing with Snappy %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
			if _, err := w.Write(msg.messageBytes); err != nil {
				logs.LogError("Error compressing with Snappy %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
		}
		if err := w.Flush(); err != nil {
			logs.LogError("Error compressing with Snappy %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		if err := w.Close(); err != nil {
			logs.LogError("Error compressing with Snappy %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}
}

func (es compressSnappy) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader {

	var zipperBuffer = make([]byte, dataSize)
	/// empty
	_, err := io.ReadFull(source, zipperBuffer)
	/// array of compress data

	if err != nil {
		logs.LogError("SNAPPY Error during reading buffer %s", err)
	}

	reader := snappy.NewReader(bytes.NewBuffer(zipperBuffer))
	if err != nil {
		logs.LogError("Error creating SNAPPY NewReader  %s", err)
	}
	defer reader.Reset(nil)
	/// headers ---> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := ioutil.ReadAll(reader)
	if err != nil {
		logs.LogError("Error during reading buffer %s", err)
	}
	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		panic("uncompressedDataSize != count")
	}

	/// headers ---> payload --> headers --> payload (compressed) --> uncompressed payload

	return bufio.NewReader(bytes.NewReader(uncompressedReader))

}

type compressLZ4 struct {
}

func (es compressLZ4) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := lz4.NewWriter(&tmp)
		for _, msg := range entry.messages {
			size := len(msg.messageBytes)
			if _, err := w.Write(bytesFromInt(uint32(size))); err != nil {
				logs.LogError("Error compressing with LZ4 %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
			if _, err := w.Write(msg.messageBytes); err != nil {
				logs.LogError("Error compressing with LZ4 %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
		}
		if err := w.Flush(); err != nil {
			logs.LogError("Error compressing with LZ4 %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		if err := w.Close(); err != nil {
			logs.LogError("Error compressing with LZ4 %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}
}

func (es compressLZ4) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader {

	var zipperBuffer = make([]byte, dataSize)
	/// empty
	_, err := io.ReadFull(source, zipperBuffer)
	/// array of compress data

	if err != nil {
		logs.LogError("LZ4 Error during reading buffer %s", err)
	}

	reader := lz4.NewReader(bytes.NewBuffer(zipperBuffer))
	defer reader.Reset(nil)
	/// headers ---> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := ioutil.ReadAll(reader)
	if err != nil {
		logs.LogError("Error during reading buffer %s", err)
	}
	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		panic("uncompressedDataSize != count")
	}

	/// headers ---> payload --> headers --> payload (compressed) --> uncompressed payload

	return bufio.NewReader(bytes.NewReader(uncompressedReader))

}

type compressZSTD struct {
}

func (es compressZSTD) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w, err := zstd.NewWriter(&tmp)
		if err != nil {
			logs.LogError("Error creating ZSTD compression algorithm writer %v", err)
			//return err  // TODO we should return error if we cannot compress
		}

		for _, msg := range entry.messages {
			size := len(msg.messageBytes)
			if _, err := w.Write(bytesFromInt(uint32(size))); err != nil {
				logs.LogError("Error compressing with ZSTD %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
			if _, err := w.Write(msg.messageBytes); err != nil {
				logs.LogError("Error compressing with ZSTD %v", err)
				//return err  // TODO we should return error if we cannot compress
			}
		}
		if err := w.Flush(); err != nil {
			logs.LogError("Error compressing with ZSTD %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		if err := w.Close(); err != nil {
			logs.LogError("Error compressing with ZSTD %v", err)
			//return err  // TODO we should return error if we cannot compress
		}
		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}
}

func (es compressZSTD) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader {

	var zipperBuffer = make([]byte, dataSize)
	/// empty
	_, err := io.ReadFull(source, zipperBuffer)
	/// array of compress data

	if err != nil {
		logs.LogError("ZSTD Error during reading buffer %s", err)
	}

	reader, err := zstd.NewReader(bytes.NewBuffer(zipperBuffer))
	if err != nil {
		logs.LogError("Error creating ZSTD NewReader  %s", err)
	}
	defer reader.Reset(nil)
	/// headers ---> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := ioutil.ReadAll(reader)
	if err != nil {
		logs.LogError("Error during reading buffer %s", err)
	}
	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		panic("uncompressedDataSize != count")
	}

	/// headers ---> payload --> headers --> payload (compressed) --> uncompressed payload

	return bufio.NewReader(bytes.NewReader(uncompressedReader))

}
