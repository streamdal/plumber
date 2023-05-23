/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package nats

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"

	"github.com/nats-io/nats.go"
)

type Sender struct {
	Conn    *nats.Conn
	Subject string

	connOwned bool
}

// NewSender creates a new protocol.Sender responsible for opening and closing the STAN connection
func NewSender(url, subject string, natsOpts []nats.Option, opts ...SenderOption) (*Sender, error) {
	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}

	s, err := NewSenderFromConn(conn, subject, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	s.connOwned = true

	return s, nil
}

// NewSenderFromConn creates a new protocol.Sender which leaves responsibility for opening and closing the STAN
// connection to the caller
func NewSenderFromConn(conn *nats.Conn, subject string, opts ...SenderOption) (*Sender, error) {
	s := &Sender{
		Conn:    conn,
		Subject: subject,
	}

	err := s.applyOptions(opts...)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Sender) Send(ctx context.Context, in binding.Message, transformers ...binding.Transformer) (err error) {
	defer func() {
		if err2 := in.Finish(err); err2 != nil {
			if err == nil {
				err = err2
			} else {
				err = fmt.Errorf("failed to call in.Finish() when error already occurred: %s: %w", err2.Error(), err)
			}
		}
	}()

	writer := new(bytes.Buffer)
	if err = WriteMsg(ctx, in, writer, transformers...); err != nil {
		return err
	}
	return s.Conn.Publish(s.Subject, writer.Bytes())
}

// Close implements Closer.Close
// This method only closes the connection if the Sender opened it
func (s *Sender) Close(_ context.Context) error {
	if s.connOwned {
		s.Conn.Close()
	}

	return nil
}

func (s *Sender) applyOptions(opts ...SenderOption) error {
	for _, fn := range opts {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}

var _ protocol.Sender = (*Sender)(nil)
var _ protocol.Closer = (*Protocol)(nil)
