package message

type StreamMessage interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	SetPublishingId(id int64)
	GetPublishingId() int64
	GetData() [][]byte
}
