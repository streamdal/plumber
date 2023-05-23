package queues_stream

import pb "github.com/kubemq-io/protobuf/go"

type SendResult struct {
	Results []*pb.SendQueueMessageResult
}

func newSendResult(results []*pb.SendQueueMessageResult) *SendResult {
	s := &SendResult{
		Results: results,
	}
	return s
}
