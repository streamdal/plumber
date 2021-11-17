package servicebus

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	common "github.com/Azure/azure-amqp-common-go/v3"
	"github.com/Azure/azure-amqp-common-go/v3/rpc"
	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	"github.com/Azure/go-amqp"
	"github.com/devigned/tab"
)

type (
	rpcClient struct {
		ec     entityConnector
		client *amqp.Client

		clientMu  sync.RWMutex
		linkCache map[string]*rpc.Link // stores 'address' => rpc.Link.

		sessionID          *string
		isSessionFilterSet bool
		cancelAuthRefresh  func() <-chan struct{}

		// replaceable for testing

		// alias of 'rpc.NewLink'
		newRPCLink func(conn *amqp.Client, address string, opts ...rpc.LinkOption) (*rpc.Link, error)

		// alias of function 'newAMQPClient'
		newAMQPClient func(ctx context.Context, ec entityConnector) (*amqp.Client, func() <-chan struct{}, error)

		// alias of 'amqp.Client.Close()'
		closeAMQPClient func() error
	}

	rpcClientOption func(*rpcClient) error
)

func newRPCClient(ctx context.Context, ec entityConnector, opts ...rpcClientOption) (*rpcClient, error) {
	r := &rpcClient{
		ec:            ec,
		linkCache:     map[string]*rpc.Link{},
		newRPCLink:    rpc.NewLink,
		newAMQPClient: newAMQPClient,
	}

	r.closeAMQPClient = func() error { return r.client.Close() }

	for _, opt := range opts {
		if err := opt(r); err != nil {
			tab.For(ctx).Error(err)
			return nil, err
		}
	}

	var err error
	r.client, r.cancelAuthRefresh, err = r.newAMQPClient(ctx, r.ec)

	if err != nil {
		tab.For(ctx).Error(err)
		return nil, err
	}
	return r, nil
}

// newAMQPClient creates a client and starts auth auto-refresh.
func newAMQPClient(ctx context.Context, ec entityConnector) (*amqp.Client, func() <-chan struct{}, error) {
	client, err := ec.Namespace().newClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	cancelAuthRefresh, err := ec.Namespace().negotiateClaim(ctx, client, ec.ManagementPath())

	if err != nil {
		client.Close()
		return nil, nil, err
	}

	return client, cancelAuthRefresh, nil
}

// Recover will attempt to close the current session and link, then rebuild them
func (r *rpcClient) Recover(ctx context.Context) error {
	ctx, span := r.startSpanFromContext(ctx, "sb.rpcClient.Recover")
	defer span.End()
	// atomically close and rebuild the client
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	_ = r.close()

	var err error
	r.client, r.cancelAuthRefresh, err = r.newAMQPClient(ctx, r.ec)

	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}
	return nil
}

// Close will close the AMQP connection
func (r *rpcClient) Close() error {
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	return r.close()
}

// closes the AMQP connection.  callers *must* hold the client write lock before calling!
func (r *rpcClient) close() error {
	if r.cancelAuthRefresh != nil {
		<-r.cancelAuthRefresh()
	}

	// we don't want to interrupt the cleanup of these links.
	r.resetLinkCache(context.Background())

	return r.closeAMQPClient()
}

// resetLinkCache removes any cached links with the assumption that we are
// recovering and do not care about errors that occur with links.
func (r *rpcClient) resetLinkCache(ctx context.Context) {
	var links []*rpc.Link

	for _, link := range r.linkCache {
		links = append(links, link)
	}

	// make a new links map - Recover() also goes through here (through `close`)
	// so we should be prepared to handle more cached links.
	r.linkCache = map[string]*rpc.Link{}

	for _, link := range links {
		link.Close(ctx)
	}
}

func (r *rpcClient) getCachedLink(ctx context.Context, address string) (*rpc.Link, error) {
	r.clientMu.RLock()
	link, ok := r.linkCache[address]
	r.clientMu.RUnlock()

	if ok {
		return link, nil
	}

	// possibly need to create the link
	r.clientMu.Lock()
	defer r.clientMu.Unlock()

	// might have been added in between us checking and
	// us getting the lock.
	link, ok = r.linkCache[address]

	if ok {
		return link, nil
	}

	link, err := r.newRPCLink(r.client, address)

	if err != nil {
		return nil, err
	}

	r.linkCache[address] = link
	return link, nil
}

// creates a new link and sends the RPC request, recovering and retrying on certain AMQP errors
func (r *rpcClient) doRPCWithRetry(ctx context.Context, address string, msg *amqp.Message, times int, delay time.Duration, opts ...rpc.LinkOption) (*rpc.Response, error) {
	// track the number of times we attempt to perform the RPC call.
	// this is to avoid a potential infinite loop if the returned error
	// is always transient and Recover() doesn't fail.
	sendCount := 0
	for {
		r.clientMu.RLock()
		client := r.client
		r.clientMu.RUnlock()
		var link *rpc.Link
		var rsp *rpc.Response
		var err error
		isCachedLink := false

		if len(opts) == 0 {
			// Can use a cached client since there's nothing unique about this particular
			// link for this call.
			// It looks like creating a management link with a session filter is the
			// only time we don't do this.
			isCachedLink = true
			link, err = r.getCachedLink(ctx, address)
		} else {
			link, err = rpc.NewLink(client, address)
		}

		if err == nil {
			defer func() {
				if isCachedLink || link == nil {
					// cached links are closed by `rpcClient.close()`
					// (which is also called during recovery)
					return
				}

				link.Close(ctx)
			}()
			rsp, err = link.RetryableRPC(ctx, times, delay, msg)
			if err == nil {
				return rsp, nil
			}
		}

		if sendCount >= amqpRetryDefaultTimes || !isAMQPTransientError(ctx, err) {
			return nil, err
		}
		sendCount++
		// if we get here, recover and try again
		tab.For(ctx).Debug("recovering RPC connection")
		_, retryErr := common.Retry(amqpRetryDefaultTimes, amqpRetryDefaultDelay, func() (interface{}, error) {
			ctx, sp := r.startProducerSpanFromContext(ctx, "sb.rpcClient.doRPCWithRetry.tryRecover")
			defer sp.End()

			if err := r.Recover(ctx); err == nil {
				tab.For(ctx).Debug("recovered RPC connection")
				return nil, nil
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, common.Retryable(err.Error())
			}
		})
		if retryErr != nil {
			tab.For(ctx).Debug("RPC recovering retried, but error was unrecoverable")
			return nil, retryErr
		}
	}
}

// returns true if the AMQP error is considered transient
func isAMQPTransientError(ctx context.Context, err error) bool {
	// always retry on a detach error
	var amqpDetach *amqp.DetachError
	if errors.As(err, &amqpDetach) {
		return true
	}
	// for an AMQP error, only retry depending on the condition
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Condition {
		case errorServerBusy, errorTimeout, errorOperationCancelled, errorContainerClose:
			return true
		default:
			tab.For(ctx).Debug(fmt.Sprintf("isAMQPTransientError: condition %s is not transient", amqpErr.Condition))
			return false
		}
	}
	tab.For(ctx).Debug(fmt.Sprintf("isAMQPTransientError: %T is not transient", err))
	return false
}

func (r *rpcClient) ReceiveDeferred(ctx context.Context, mode ReceiveMode, sequenceNumbers ...int64) ([]*Message, error) {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.rpcClient.ReceiveDeferred")
	defer span.End()

	const messagesField, messageField = "messages", "message"

	backwardsMode := uint32(0)
	if mode == PeekLockMode {
		backwardsMode = 1
	}

	values := map[string]interface{}{
		"sequence-numbers":     sequenceNumbers,
		"receiver-settle-mode": uint32(backwardsMode), // pick up messages with peek lock
	}

	var opts []rpc.LinkOption
	if r.isSessionFilterSet {
		opts = append(opts, rpc.LinkWithSessionFilter(r.sessionID))
		values["session-id"] = r.sessionID
	}

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: "com.microsoft:receive-by-sequence-number",
		},
		Value: values,
	}

	rsp, err := r.doRPCWithRetry(ctx, r.ec.ManagementPath(), msg, 5, 5*time.Second, opts...)
	if err != nil {
		tab.For(ctx).Error(err)
		return nil, err
	}

	if rsp.Code == 204 {
		return nil, ErrNoMessages{}
	}

	// Deferred messages come back in a relatively convoluted manner:
	// a map (always with one key: "messages")
	// 	of arrays
	// 		of maps (always with one key: "message")
	// 			of an array with raw encoded Service Bus messages
	val, ok := rsp.Message.Value.(map[string]interface{})
	if !ok {
		return nil, newErrIncorrectType(messageField, map[string]interface{}{}, rsp.Message.Value)
	}

	rawMessages, ok := val[messagesField]
	if !ok {
		return nil, ErrMissingField(messagesField)
	}

	messages, ok := rawMessages.([]interface{})
	if !ok {
		return nil, newErrIncorrectType(messagesField, []interface{}{}, rawMessages)
	}

	transformedMessages := make([]*Message, len(messages))
	for i := range messages {
		rawEntry, ok := messages[i].(map[string]interface{})
		if !ok {
			return nil, newErrIncorrectType(messageField, map[string]interface{}{}, messages[i])
		}

		rawMessage, ok := rawEntry[messageField]
		if !ok {
			return nil, ErrMissingField(messageField)
		}

		marshaled, ok := rawMessage.([]byte)
		if !ok {
			return nil, new(ErrMalformedMessage)
		}

		var rehydrated amqp.Message
		err = rehydrated.UnmarshalBinary(marshaled)
		if err != nil {
			return nil, err
		}

		transformedMessages[i], err = messageFromAMQPMessage(&rehydrated, nil)
		if err != nil {
			return nil, err
		}

		transformedMessages[i].ec = r.ec
		transformedMessages[i].useSession = r.isSessionFilterSet
		transformedMessages[i].sessionID = r.sessionID
	}

	// This sort is done to ensure that folks wanting to peek messages in sequence order may do so.
	sort.Slice(transformedMessages, func(i, j int) bool {
		iSeq := *transformedMessages[i].SystemProperties.SequenceNumber
		jSeq := *transformedMessages[j].SystemProperties.SequenceNumber
		return iSeq < jSeq
	})

	return transformedMessages, nil
}

func (r *rpcClient) GetNextPage(ctx context.Context, fromSequenceNumber int64, messageCount int32) ([]*Message, error) {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.rpcClient.GetNextPage")
	defer span.End()

	const messagesField, messageField = "messages", "message"

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: peekMessageOperationID,
		},
		Value: map[string]interface{}{
			"from-sequence-number": fromSequenceNumber,
			"message-count":        messageCount,
		},
	}

	if deadline, ok := ctx.Deadline(); ok {
		msg.ApplicationProperties["server-timeout"] = uint(time.Until(deadline) / time.Millisecond)
	}

	rsp, err := r.doRPCWithRetry(ctx, r.ec.ManagementPath(), msg, 5, 5*time.Second)
	if err != nil {
		tab.For(ctx).Error(err)
		return nil, err
	}

	if rsp.Code == 204 {
		return nil, ErrNoMessages{}
	}

	// Peeked messages come back in a relatively convoluted manner:
	// a map (always with one key: "messages")
	// 	of arrays
	// 		of maps (always with one key: "message")
	// 			of an array with raw encoded Service Bus messages
	val, ok := rsp.Message.Value.(map[string]interface{})
	if !ok {
		err = newErrIncorrectType(messageField, map[string]interface{}{}, rsp.Message.Value)
		tab.For(ctx).Error(err)
		return nil, err
	}

	rawMessages, ok := val[messagesField]
	if !ok {
		err = ErrMissingField(messagesField)
		tab.For(ctx).Error(err)
		return nil, err
	}

	messages, ok := rawMessages.([]interface{})
	if !ok {
		err = newErrIncorrectType(messagesField, []interface{}{}, rawMessages)
		tab.For(ctx).Error(err)
		return nil, err
	}

	transformedMessages := make([]*Message, len(messages))
	for i := range messages {
		rawEntry, ok := messages[i].(map[string]interface{})
		if !ok {
			err = newErrIncorrectType(messageField, map[string]interface{}{}, messages[i])
			tab.For(ctx).Error(err)
			return nil, err
		}

		rawMessage, ok := rawEntry[messageField]
		if !ok {
			err = ErrMissingField(messageField)
			tab.For(ctx).Error(err)
			return nil, err
		}

		marshaled, ok := rawMessage.([]byte)
		if !ok {
			err = new(ErrMalformedMessage)
			tab.For(ctx).Error(err)
			return nil, err
		}

		var rehydrated amqp.Message
		err = rehydrated.UnmarshalBinary(marshaled)
		if err != nil {
			tab.For(ctx).Error(err)
			return nil, err
		}

		transformedMessages[i], err = messageFromAMQPMessage(&rehydrated, nil)
		if err != nil {
			tab.For(ctx).Error(err)
			return nil, err
		}

		transformedMessages[i].ec = r.ec
		transformedMessages[i].useSession = r.isSessionFilterSet
		transformedMessages[i].sessionID = r.sessionID
	}

	// This sort is done to ensure that folks wanting to peek messages in sequence order may do so.
	sort.Slice(transformedMessages, func(i, j int) bool {
		iSeq := *transformedMessages[i].SystemProperties.SequenceNumber
		jSeq := *transformedMessages[j].SystemProperties.SequenceNumber
		return iSeq < jSeq
	})

	return transformedMessages, nil
}

func (r *rpcClient) RenewLocks(ctx context.Context, messages ...*Message) error {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.RenewLocks")
	defer span.End()

	var linkName string
	lockTokens := make([]amqp.UUID, 0, len(messages))
	for _, m := range messages {
		if m.LockToken == nil {
			tab.For(ctx).Error(fmt.Errorf("failed: message has nil lock token, cannot renew lock"), tab.StringAttribute("messageId", m.ID))
			continue
		}

		amqpLockToken := amqp.UUID(*m.LockToken)
		lockTokens = append(lockTokens, amqpLockToken)
		if linkName == "" {
			linkName = m.getLinkName()
		}
	}

	if len(lockTokens) < 1 {
		tab.For(ctx).Info("no lock tokens present to renew")
		return nil
	}

	renewRequestMsg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: lockRenewalOperationName,
		},
		Value: map[string]interface{}{
			lockTokensFieldName: lockTokens,
		},
	}
	if linkName != "" {
		renewRequestMsg.ApplicationProperties[associatedLinkName] = linkName
	}

	response, err := r.doRPCWithRetry(ctx, r.ec.ManagementPath(), renewRequestMsg, 3, 1*time.Second)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	if response.Code != 200 {
		err := fmt.Errorf("error renewing locks: %v", response.Description)
		tab.For(ctx).Error(err)
		return err
	}

	return nil
}

func (r *rpcClient) SendDisposition(ctx context.Context, m *Message, state disposition) error {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.rpcClient.SendDisposition")
	defer span.End()

	if m.LockToken == nil {
		err := errors.New("lock token on the message is not set, thus cannot send disposition")
		tab.For(ctx).Error(err)
		return err
	}

	var opts []rpc.LinkOption
	value := map[string]interface{}{
		"disposition-status": string(state.Status),
		"lock-tokens":        []amqp.UUID{amqp.UUID(*m.LockToken)},
	}

	if state.DeadLetterReason != nil {
		value["deadletter-reason"] = state.DeadLetterReason
	}

	if state.DeadLetterDescription != nil {
		value["deadletter-description"] = state.DeadLetterDescription
	}

	if m.useSession {
		value["session-id"] = m.sessionID
		opts = append(opts, rpc.LinkWithSessionFilter(m.sessionID))
	}

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: "com.microsoft:update-disposition",
		},
		Value: value,
	}

	// no error, then it was successful
	_, err := r.doRPCWithRetry(ctx, m.ec.ManagementPath(), msg, 5, 5*time.Second, opts...)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	return nil
}

// ScheduleAt will send a batch of messages to a Queue, schedule them to be enqueued, and return the sequence numbers
// that can be used to cancel each message.
func (r *rpcClient) ScheduleAt(ctx context.Context, enqueueTime time.Time, messages ...*Message) ([]int64, error) {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.rpcClient.ScheduleAt")
	defer span.End()

	if len(messages) <= 0 {
		return nil, errors.New("expected one or more messages")
	}

	transformed := make([]interface{}, 0, len(messages))
	for i := range messages {
		messages[i].ScheduleAt(enqueueTime)

		if messages[i].ID == "" {
			id, err := uuid.NewV4()
			if err != nil {
				return nil, err
			}
			messages[i].ID = id.String()
		}

		rawAmqp, err := messages[i].toMsg()
		if err != nil {
			return nil, err
		}
		encoded, err := rawAmqp.MarshalBinary()
		if err != nil {
			return nil, err
		}

		individualMessage := map[string]interface{}{
			"message-id": messages[i].ID,
			"message":    encoded,
		}
		if messages[i].SessionID != nil {
			individualMessage["session-id"] = *messages[i].SessionID
		}
		if partitionKey := messages[i].SystemProperties.PartitionKey; partitionKey != nil {
			individualMessage["partition-key"] = *partitionKey
		}
		if viaPartitionKey := messages[i].SystemProperties.ViaPartitionKey; viaPartitionKey != nil {
			individualMessage["via-partition-key"] = *viaPartitionKey
		}

		transformed = append(transformed, individualMessage)
	}

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: scheduleMessageOperationID,
		},
		Value: map[string]interface{}{
			"messages": transformed,
		},
	}

	if deadline, ok := ctx.Deadline(); ok {
		msg.ApplicationProperties[serverTimeoutFieldName] = uint(time.Until(deadline) / time.Millisecond)
	}

	resp, err := r.doRPCWithRetry(ctx, r.ec.ManagementPath(), msg, 5, 5*time.Second)
	if err != nil {
		tab.For(ctx).Error(err)
		return nil, err
	}

	if resp.Code != 200 {
		return nil, ErrAMQP(*resp)
	}

	retval := make([]int64, 0, len(messages))
	if rawVal, ok := resp.Message.Value.(map[string]interface{}); ok {
		const sequenceFieldName = "sequence-numbers"
		if rawArr, ok := rawVal[sequenceFieldName]; ok {
			if arr, ok := rawArr.([]int64); ok {
				for i := range arr {
					retval = append(retval, arr[i])
				}
				return retval, nil
			}
			return nil, newErrIncorrectType(sequenceFieldName, []int64{}, rawArr)
		}
		return nil, ErrMissingField(sequenceFieldName)
	}
	return nil, newErrIncorrectType("value", map[string]interface{}{}, resp.Message.Value)
}

// CancelScheduled allows for removal of messages that have been handed to the Service Bus broker for later delivery,
// but have not yet ben enqueued.
func (r *rpcClient) CancelScheduled(ctx context.Context, seq ...int64) error {
	ctx, span := startConsumerSpanFromContext(ctx, "sb.rpcClient.CancelScheduled")
	defer span.End()

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationFieldName: cancelScheduledOperationID,
		},
		Value: map[string]interface{}{
			"sequence-numbers": seq,
		},
	}

	if deadline, ok := ctx.Deadline(); ok {
		msg.ApplicationProperties[serverTimeoutFieldName] = uint(time.Until(deadline) / time.Millisecond)
	}

	resp, err := r.doRPCWithRetry(ctx, r.ec.ManagementPath(), msg, 5, 5*time.Second)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	if resp.Code != 200 {
		return ErrAMQP(*resp)
	}

	return nil
}

func rpcClientWithSession(sessionID *string) rpcClientOption {
	return func(r *rpcClient) error {
		r.sessionID = sessionID
		r.isSessionFilterSet = true
		return nil
	}
}
