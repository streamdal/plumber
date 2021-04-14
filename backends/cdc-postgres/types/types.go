package types

// RelayMessage encapsulates a kafka message that is read by relay.Run()
type RelayMessage struct {
	Value   *ChangeRecord
	Options *RelayMessageOptions
}

type ChangeRecord struct {
	LSN       string    `json:"lsn"`
	Timestamp int64     `json:"timestamp"`
	Changes   []*Change `json:"changes"`
}

type Change struct {
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Fields    map[string]interface{} `json:"fields"`
}

// RelayMessageOptions contains any additional options necessary for processing of Kafka messages by the relayer
type RelayMessageOptions struct {
}
