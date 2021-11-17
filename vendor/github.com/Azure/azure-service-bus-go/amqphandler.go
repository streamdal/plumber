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

	"github.com/Azure/go-amqp"
	"github.com/devigned/tab"
)

type amqpHandler interface {
	Handle(ctx context.Context, msg *amqp.Message, r *amqp.Receiver) error
}

// amqpAdapterHandler is a middleware handler that translates amqp messages into servicebus messages
type amqpAdapterHandler struct {
	next     Handler
	receiver *Receiver
}

func newAmqpAdapterHandler(receiver *Receiver, next Handler) *amqpAdapterHandler {
	return &amqpAdapterHandler{
		next:     next,
		receiver: receiver,
	}
}

func (h *amqpAdapterHandler) Handle(ctx context.Context, msg *amqp.Message, r *amqp.Receiver) error {
	const optName = "sb.amqpHandler.Handle"

	event, err := messageFromAMQPMessage(msg, r)
	if err != nil {
		_, span := h.receiver.startConsumerSpanFromContext(ctx, optName)
		span.Logger().Error(err)
		h.receiver.lastError = err
		if h.receiver.doneListening != nil {
			h.receiver.doneListening()
		}
		return err
	}

	ctx, span := tab.StartSpanWithRemoteParent(ctx, optName, event)
	defer span.End()

	id := messageID(msg)
	if idStr, ok := id.(string); ok {
		span.AddAttributes(tab.StringAttribute("amqp.message.id", idStr))
	}

	if err := h.next.Handle(ctx, event); err != nil {
		// stop handling messages since the message consumer ran into an unexpected error
		h.receiver.lastError = err
		if h.receiver.doneListening != nil {
			h.receiver.doneListening()
		}
		return err
	}

	// nothing more to be done. The message was settled when it was accepted by the Receiver
	if h.receiver.mode == ReceiveAndDeleteMode {
		return nil
	}

	// nothing more to be done. The Receiver has no default disposition, so the handler is solely responsible for
	// disposition
	if h.receiver.DefaultDisposition == nil {
		return nil
	}

	// default disposition is set, so try to send the disposition. If the message disposition has already been set, the
	// underlying AMQP library will ignore the second disposition respecting the disposition of the handler func.
	if err := h.receiver.DefaultDisposition(ctx); err != nil {
		// if an error is returned by the default disposition, then we must alert the message consumer as we can't
		// be sure the final message disposition.
		tab.For(ctx).Error(err)
		h.receiver.lastError = err
		if h.receiver.doneListening != nil {
			h.receiver.doneListening()
		}
		return nil
	}
	return nil
}
