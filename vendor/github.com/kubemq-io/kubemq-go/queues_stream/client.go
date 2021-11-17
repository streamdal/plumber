package queues_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueuesStreamClient struct {
	clientCtx  context.Context
	client     *GrpcClient
	upstream   *upstream
	downstream *downstream
}

func NewQueuesStreamClient(ctx context.Context, op ...Option) (*QueuesStreamClient, error) {
	client, err := NewGrpcClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	c := &QueuesStreamClient{
		clientCtx: ctx,
		client:    client,
	}
	c.upstream = newUpstream(ctx, c)
	c.downstream = newDownstream(ctx, c)
	return c, nil
}
func (q *QueuesStreamClient) Send(ctx context.Context, messages ...*QueueMessage) (*SendResult, error) {
	if !q.upstream.isReady() {
		return nil, fmt.Errorf("kubemq grpc client connection lost, can't send messages ")
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}
	var list []*pb.QueueMessage
	for _, message := range messages {
		list = append(list, message.complete(q.client.GlobalClientId()).QueueMessage)
	}
	req := &pb.QueuesUpstreamRequest{
		RequestID: uuid.New(),
		Messages:  list,
	}
	select {
	case resp := <-q.upstream.send(req):
		if resp.IsError {
			return nil, fmt.Errorf(resp.Error)
		}
		return newSendResult(resp.Results), nil
	case <-ctx.Done():
		q.upstream.cancelTransaction(req.RequestID)
		return nil, ctx.Err()
	}
}

func (q *QueuesStreamClient) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	if !q.downstream.isReady() {
		return nil, fmt.Errorf("kubemq grpc client connection lost, can't poll messages")
	}
	pollReq, err := q.downstream.poll(ctx, request, q.client.GlobalClientId())
	return pollReq, err
}

func (q *QueuesStreamClient) AckAll(ctx context.Context, request *AckAllRequest) (*AckAllResponse, error) {
	if err := request.validateAndComplete(q.client.GlobalClientId()); err != nil {
		return nil, err
	}
	req := &pb.AckAllQueueMessagesRequest{
		RequestID:       request.requestID,
		ClientID:        request.ClientID,
		Channel:         request.Channel,
		WaitTimeSeconds: request.WaitTimeSeconds,
	}
	pbResp, err := q.client.AckAllQueueMessages(ctx, req)
	if err != nil {
		return nil, err
	}
	resp := &AckAllResponse{
		RequestID:        pbResp.RequestID,
		AffectedMessages: pbResp.AffectedMessages,
		IsError:          pbResp.IsError,
		Error:            pbResp.Error,
	}
	return resp, nil
}

func (q *QueuesStreamClient) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	resp, err := q.client.QueuesInfo(ctx, &pb.QueuesInfoRequest{
		RequestID: uuid.New(),
		QueueName: filter,
	})
	if err != nil {
		return nil, err
	}
	return fromQueuesInfoPb(resp.Info), nil
}

func (q *QueuesStreamClient) Close() error {
	time.Sleep(100 * time.Millisecond)
	q.upstream.close()
	q.downstream.close()
	return q.client.Close()
}
