package pb

// MDType is used for type safety on message descriptor maps used in protobuf functions
type MDType string

var (
	MDEnvelope MDType = "envelope"
	MDPayload  MDType = "payload"
)
