package stream

type Event struct {
	Command    uint16
	StreamName string
	Name       string
	Reason     string
	Err        error
}

type metaDataUpdateEvent struct {
	StreamName string
	code       uint16
}

type PublishError struct {
	Code               uint16
	Err                error
	UnConfirmedMessage *UnConfirmedMessage
}

type onInternalClose func(ch <-chan uint8)
type metadataListener chan metaDataUpdateEvent

type ChannelClose = <-chan Event
type ChannelPublishError = chan PublishError
type ChannelPublishConfirm chan []*UnConfirmedMessage
