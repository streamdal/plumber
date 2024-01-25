package main

import (
	"io/ioutil"

	"github.com/streamdal/plumber/test-assets/shallow-envelope/shallow"

	"github.com/golang/protobuf/proto"
)

func main() {
	payload := &shallow.Payload{
		Name: "Mark",
	}

	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}

	envelope := &shallow.Envelope{
		Id:   "test-1",
		Data: payloadBytes,
	}

	envelopeBytes, err := proto.Marshal(envelope)
	if err != nil {
		panic(err)
	}

	ioutil.WriteFile("payload.bin", envelopeBytes, 0644)
	println("write data to payload.bin")
}
