package types

import (
	"bytes"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
)

type RuleSet struct {
	Set *common.RuleSet `json:"set"`
}

// MarshalJSON marshals a ruleset to JSON
func (r *RuleSet) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(``))

	m := jsonpb.Marshaler{
		EnumsAsInts: false,
		OrigName:    true,
	}
	if err := m.Marshal(buf, r.Set); err != nil {
		return nil, errors.Wrap(err, "could not marshal opts.TunnelOptions")
	}

	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a ruleset struct
func (r *RuleSet) UnmarshalJSON(v []byte) error {
	rs := &common.RuleSet{}

	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), rs); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored ruleset")
	}

	r.Set = rs

	return nil
}
