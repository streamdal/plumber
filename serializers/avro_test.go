package serializers

import (
	"bytes"
	"testing"
)

var avroSchema = []byte(`
	{
		 "type": "record",
		 "namespace": "com.example",
		 "name": "Companies",
		 "fields": [
		   { "name": "company", "type": "string" }
		 ]
	}`)

var plainData = []byte(`{"company":"Batch Corp"}`)
var encodedAvroData = []byte{0x14, 0x42, 0x61, 0x74, 0x63, 0x68, 0x20, 0x43, 0x6f, 0x72, 0x70}

func TestAvroEncode(t *testing.T) {
	got, err := AvroEncode(avroSchema, plainData)
	if err != nil {
		t.Errorf("AvroEncode returned error %s", err.Error())
	}
	if !bytes.Equal(encodedAvroData, got) {
		t.Errorf("AvroEncode returned %#v", got)
	}
}

func TestAvroDecode(t *testing.T) {
	got, err := AvroDecode(avroSchema, encodedAvroData)
	if err != nil {
		t.Errorf("AvroDecode returned error %s", err.Error())
	}

	if !bytes.Equal(got, plainData) {
		t.Errorf("AvroDecode returned %s", got)
	}
}
