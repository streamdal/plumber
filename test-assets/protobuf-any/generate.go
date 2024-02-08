package main

import (
	"os"

	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/streamdal/plumber/test-assets/protobuf-any/sample"
)

func main() {
	inner, err := anypb.New(&sample.Message{
		Name: "Mark",
		Age:  39,
	})
	if err != nil {
		panic(err)
	}

	m := &sample.Envelope{
		Message: "Plumber supports google.protobuf.Any",
		Details: inner,
	}

	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}

	os.WriteFile("payload.bin", data, 0644)
	println("write data to payload.bin")
}
