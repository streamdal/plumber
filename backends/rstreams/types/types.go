package types

type RelayMessage struct {
	ID     string
	Stream string
	Key    string
	Value  []byte
}
