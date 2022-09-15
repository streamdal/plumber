package stream

//go:generate mockgen . BaseEnvironment
type BaseEnvironment interface {
	DeclareStream(streamName string, options *StreamOptions) error
	DeleteStream(streamName string) error
	NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error)
	StreamExists(streamName string) (bool, error)
	QueryOffset(consumerName string, streamName string) (int64, error)
	QuerySequence(publisherReference string, streamName string) (int64, error)
	StreamMetaData(streamName string) (*StreamMetadata, error)
	NewConsumer(streamName string,
		messagesHandler MessagesHandler,
		options *ConsumerOptions) (*Consumer, error)
}
