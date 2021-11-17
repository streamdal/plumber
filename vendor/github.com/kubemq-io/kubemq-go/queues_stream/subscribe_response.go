package queues_stream

type SubscribeResponse struct {
	Messages []*QueueMessage
	*responseHandler
}

func NewSubscribeResponse() *SubscribeResponse {
	return &SubscribeResponse{}
}
func (s *SubscribeResponse) setMessages(messages []*QueueMessage) *SubscribeResponse {
	s.Messages = messages
	return s
}
func (s *SubscribeResponse) setResponseHandler(handler *responseHandler) *SubscribeResponse {
	s.responseHandler = handler
	return s
}
