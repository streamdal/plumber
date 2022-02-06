package types

import (
	"bytes"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
)

// Connection is a convenience struct wrapping opts.ConnectionOptions so we can marshal
// Config to JSON. The Marshaler methods are implemented to return a string of protobuf bytes
// because protobuf oneof's cannot be unmarshalled correctly by json.Unmarshal()
type Connection struct {
	Connection *opts.ConnectionOptions
}

func (c *Connection) MarshalJSON() ([]byte, error) {
	m := jsonpb.Marshaler{}

	buf := bytes.NewBuffer([]byte(``))

	if err := m.Marshal(buf, c.Connection); err != nil {
		return nil, errors.Wrap(err, "could not marshal protos.Connection")
	}
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a connection struct
func (c *Connection) UnmarshalJSON(v []byte) error {
	conn := &opts.ConnectionOptions{}

	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), conn); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored connection")
	}

	c.Connection = conn

	return nil
}
