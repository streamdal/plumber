package queues_stream

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"io"
	"sync"
	"time"
)

type downstream struct {
	sync.Mutex
	downstreamCtx       context.Context
	downstreamCancel    context.CancelFunc
	pendingTransactions map[string]*responseHandler
	activeTransactions  map[string]*responseHandler
	requestCh           chan *pb.QueuesDownstreamRequest
	responseCh          chan *pb.QueuesDownstreamResponse
	errCh               chan error
	doneCh              chan bool
	grpcClient          pb.KubemqClient
	streamClient        *QueuesStreamClient
	isClosed            bool
	connectionState     *atomic.Bool
}

func newDownstream(ctx context.Context, streamClient *QueuesStreamClient) *downstream {

	d := &downstream{
		Mutex:               sync.Mutex{},
		downstreamCtx:       nil,
		downstreamCancel:    nil,
		pendingTransactions: map[string]*responseHandler{},
		activeTransactions:  map[string]*responseHandler{},
		requestCh:           make(chan *pb.QueuesDownstreamRequest, 10),
		responseCh:          make(chan *pb.QueuesDownstreamResponse, 10),
		errCh:               make(chan error, 10),
		doneCh:              make(chan bool, 1),
		grpcClient:          streamClient.client.KubemqClient,
		streamClient:        streamClient,
		isClosed:            false,
		connectionState:     atomic.NewBool(false),
	}
	d.downstreamCtx, d.downstreamCancel = context.WithCancel(ctx)
	go d.run()
	time.Sleep(time.Second)
	return d
}

func (d *downstream) close() {
	d.setIsClose(true)
	d.downstreamCancel()
}
func (d *downstream) setIsClose(value bool) {
	d.Lock()
	defer d.Unlock()
	d.isClosed = value
}
func (d *downstream) getIsClose() bool {
	d.Lock()
	defer d.Unlock()
	return d.isClosed
}
func (d *downstream) sendOnConnectionState(msg string) {
	if d.streamClient.client.opts.connectionNotificationFunc != nil {
		go func() {
			d.streamClient.client.opts.connectionNotificationFunc(msg)
		}()
	}
}
func (d *downstream) createPendingTransaction(request *pb.QueuesDownstreamRequest) *responseHandler {
	d.Lock()
	defer d.Unlock()
	handler := newResponseHandler().
		setRequestId(request.RequestID).
		setRequestChanel(request.Channel).
		setRequestClientId(request.ClientID).
		setRequestCh(d.requestCh)
	d.pendingTransactions[request.RequestID] = handler
	return handler
}
func (d *downstream) movePendingToActiveTransaction(requestId, transactionId string) (*responseHandler, bool) {
	d.Lock()
	defer d.Unlock()
	handler, ok := d.pendingTransactions[requestId]
	if ok {
		handler.transactionId = transactionId
		d.activeTransactions[transactionId] = handler
		delete(d.pendingTransactions, requestId)
		return handler, true
	} else {
		return nil, false
	}
}
func (d *downstream) deletePendingTransaction(requestId string) {
	d.Lock()
	defer d.Unlock()
	delete(d.pendingTransactions, requestId)
	//log.Println("pending transaction deleted", requestId)
}

func (d *downstream) getActiveTransaction(id string) (*responseHandler, bool) {
	d.Lock()
	defer d.Unlock()
	handler, ok := d.activeTransactions[id]
	return handler, ok
}

func (d *downstream) deleteActiveTransaction(id string) {
	d.Lock()
	defer d.Unlock()
	delete(d.activeTransactions, id)
	//log.Println("active transaction deleted", id)
}
func (d *downstream) connectStream(ctx context.Context) {
	defer func() {
		d.doneCh <- true
		d.connectionState.Store(false)
		d.sendOnConnectionState(fmt.Sprintf("grpc queue client downstream disconnected"))
	}()
	stream, err := d.grpcClient.QueuesDownstream(ctx)
	if err != nil {
		d.errCh <- err
		d.sendOnConnectionState(fmt.Sprintf("grpc queue client downstream connection error, %s", err.Error()))
		return
	}
	d.connectionState.Store(true)
	d.sendOnConnectionState(fmt.Sprintf("grpc queue client downstream connected"))
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				d.errCh <- err
				d.sendOnConnectionState(fmt.Sprintf("grpc queue client downstream receive error, %s", err.Error()))
				return
			}
			select {
			case d.responseCh <- res:
			case <-stream.Context().Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case req := <-d.requestCh:
			err := stream.Send(req)
			if err != nil {
				if err == io.EOF {
					return
				}
				d.errCh <- err
				d.sendOnConnectionState(fmt.Sprintf("grpc queue client downstream send error, %s", err.Error()))
				return
			}
		case <-stream.Context().Done():
			return
		case <-ctx.Done():
			return
		}
	}

}
func (d *downstream) clearPendingTransactions(err error) {
	d.Lock()
	d.Unlock()
	for id, handler := range d.pendingTransactions {
		select {
		case handler.errCh <- err:
		default:

		}
		handler.sendError(err)
		handler.sendComplete()
		delete(d.pendingTransactions, id)
	}
}
func (d *downstream) clearActiveTransactions(err error) {
	d.Lock()
	d.Unlock()
	for id, handler := range d.activeTransactions {
		select {
		case handler.errCh <- err:
		default:
		}
		handler.sendError(err)
		handler.sendComplete()
		delete(d.activeTransactions, id)
	}
}
func (d *downstream) run() {
	for {
		if !d.getIsClose() {
			go d.connectStream(d.downstreamCtx)
		} else {
			return
		}
		for {
			select {
			case resp := <-d.responseCh:
				if resp.RequestTypeData == pb.QueuesDownstreamRequestType_Get {
					handler, ok := d.movePendingToActiveTransaction(resp.RefRequestId, resp.TransactionId)
					if ok {
						handler.responseCh <- resp
					}
				} else {
					handler, ok := d.getActiveTransaction(resp.TransactionId)
					if ok {
						if resp.TransactionComplete {
							handler.sendComplete()
							d.deleteActiveTransaction(resp.TransactionId)
							continue
						}
						if resp.IsError {
							handler.sendError(fmt.Errorf(resp.Error))
							continue
						}
					}
				}
			case err := <-d.errCh:
				d.clearPendingTransactions(err)
				d.clearActiveTransactions(err)
			case <-d.doneCh:
				goto reconnect
			case <-d.downstreamCtx.Done():
				d.clearPendingTransactions(d.downstreamCtx.Err())
				d.clearActiveTransactions(d.downstreamCtx.Err())
				return
			}
		}
	reconnect:
		time.Sleep(time.Second)
	}
}
func (d *downstream) isReady() bool {
	return d.connectionState.Load()
}
func (d *downstream) poll(ctx context.Context, request *PollRequest, clientId string) (*PollResponse, error) {
	pbReq, err := request.validateAndComplete(clientId)
	if err != nil {
		return nil, err
	}
	respHandler := d.createPendingTransaction(pbReq).
		setOnErrorFunc(request.OnErrorFunc).
		setOnCompleteFunc(request.OnComplete)
	select {
	case d.requestCh <- pbReq:
	case <-time.After(time.Second):
		return nil, fmt.Errorf("sending poll request timout error")
	}

	waitFirstResponse := request.WaitTimeout
	if waitFirstResponse == 0 {
		waitFirstResponse = 60000
	} else {
		waitFirstResponse = request.WaitTimeout + 1000
	}
	waitResponseCtx, waitResponseCancel := context.WithTimeout(ctx, time.Duration(waitFirstResponse)*time.Millisecond)
	defer waitResponseCancel()
	select {
	case resp := <-respHandler.responseCh:
		if resp.IsError {
			return nil, fmt.Errorf(resp.Error)
		}
		pollResponse := newPollResponse(resp.Messages, respHandler)
		if len(pollResponse.Messages) > 0 && !pbReq.AutoAck {
			respHandler.start(ctx)
		} else {
			respHandler.sendComplete()
			d.deleteActiveTransaction(resp.TransactionId)
		}
		return pollResponse, nil
	case connectionErr := <-respHandler.errCh:
		return nil, fmt.Errorf("grpcClient connection error, %s", connectionErr.Error())
	case <-waitResponseCtx.Done():
		d.deletePendingTransaction(pbReq.RequestID)
		return nil, fmt.Errorf("timout waiting response for poll messages request")
	case <-d.downstreamCtx.Done():
		respHandler.sendError(d.downstreamCtx.Err())
		respHandler.sendComplete()
		d.deletePendingTransaction(pbReq.RequestID)
		return nil, d.downstreamCtx.Err()
	case <-ctx.Done():
		respHandler.sendError(ctx.Err())
		respHandler.sendComplete()
		d.deletePendingTransaction(pbReq.RequestID)
		return nil, ctx.Err()
	}
}
