package serializers

import (
	"bytes"
	"io/ioutil"
	"testing"
)

var schemaFile = "../test-assets/avro/test.avsc"
var dataFile = "../test-assets/avro/test.json"

var encodedAvroData = []byte{0x14, 0x42, 0x61, 0x74, 0x63, 0x68, 0x20, 0x43, 0x6f, 0x72, 0x70}

func TestAvroEncode(t *testing.T) {
	data, err := ioutil.ReadFile(dataFile)
	if err != nil {
		t.Fatalf("Unable to read test data file %s", dataFile)
	}
	got, err := AvroEncodeWithSchemaFile(schemaFile, data)
	if err != nil {
		t.Errorf("AvroEncode returned error %s", err.Error())
	}
	if !bytes.Equal(encodedAvroData, got) {
		t.Errorf("AvroEncode returned %#v", got)
	}
}

func TestAvroDecode(t *testing.T) {

	data, err := ioutil.ReadFile(dataFile)
	if err != nil {
		t.Fatalf("Unable to read test data file %s", dataFile)
	}

	got, err := AvroDecodeWithSchemaFile(schemaFile, encodedAvroData)
	if err != nil {
		t.Errorf("AvroDecode returned error %s", err.Error())
	}

	if !bytes.Equal(got, data) {
		t.Errorf("AvroDecode returned %s", got)
	}
}
