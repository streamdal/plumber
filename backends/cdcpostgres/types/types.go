package types

// RelayMessage encapsulates a ChangeRecord message that is read by relay.Run()
type RelayMessage struct {
	Value   *ChangeRecord
	Options *RelayMessageOptions
}

// ChangeRecord represents a single change to a table
type ChangeRecord struct {
	LSN       string                 `json:"lsn"`
	XID       int32                  `json:"xid"`
	Timestamp int64                  `json:"timestamp"`
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Fields    map[string]interface{} `json:"fields"`
	OldFields map[string]interface{} `json:"old_fields,omitempty"`
}

// RelayMessageOptions contains any additional options necessary for processing of ChangeRecord messages by the relayer
type RelayMessageOptions struct {
}
