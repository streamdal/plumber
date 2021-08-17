package stomp

import (
	"github.com/go-stomp/stomp/v3/frame"
)

// A Transaction applies to the sending of messages to the STOMP server,
// and the acknowledgement of messages received from the STOMP server.
// All messages sent and and acknowledged in the context of a transaction
// are processed atomically by the STOMP server.
//
// Transactions are committed with the Commit method. When a transaction is
// committed, all sent messages, acknowledgements and negative acknowledgements,
// are processed by the STOMP server. Alternatively transactions can be aborted,
// in which case all sent messages, acknowledgements and negative
// acknowledgements are discarded by the STOMP server.
type Transaction struct {
	id        string
	conn      *Conn
	completed bool
}

// Id returns the unique identifier for the transaction.
func (tx *Transaction) Id() string {
	return tx.id
}

// Conn returns the connection associated with this transaction.
func (tx *Transaction) Conn() *Conn {
	return tx.conn
}

// Abort will abort the transaction. Any calls to Send, SendWithReceipt,
// Ack and Nack on this transaction will be discarded.
// This function does not wait for the server to process the ABORT frame.
// See AbortWithReceipt if you want to ensure the ABORT is processed.
func (tx *Transaction) Abort() error {
	return tx.abort(false)
}

// Abort will abort the transaction. Any calls to Send, SendWithReceipt,
// Ack and Nack on this transaction will be discarded.
func (tx *Transaction) AbortWithReceipt() error {
	return tx.abort(true)
}

func (tx *Transaction) abort(receipt bool) error {
	if tx.completed {
		return ErrCompletedTransaction
	}

	f := frame.New(frame.ABORT, frame.Transaction, tx.id)

	if receipt {
		id := allocateId()
		f.Header.Set(frame.Receipt, id)
	}

	err := tx.conn.sendFrame(f)
	if err != nil {
		return err
	}
	tx.completed = true

	return nil
}

// Commit will commit the transaction. All messages and acknowledgements
// sent to the STOMP server on this transaction will be processed atomically.
// This function does not wait for the server to process the COMMIT frame.
// See CommitWithReceipt if you want to ensure the COMMIT is processed.
func (tx *Transaction) Commit() error {
	return tx.commit(false)
}

// Commit will commit the transaction. All messages and acknowledgements
// sent to the STOMP server on this transaction will be processed atomically.
func (tx *Transaction) CommitWithReceipt() error {
	return tx.commit(true)
}

func (tx *Transaction) commit(receipt bool) error {
	if tx.completed {
		return ErrCompletedTransaction
	}

	f := frame.New(frame.COMMIT, frame.Transaction, tx.id)

	if receipt {
		id := allocateId()
		f.Header.Set(frame.Receipt, id)
	}

	err := tx.conn.sendFrame(f)
	if err != nil {
		return err
	}
	tx.completed = true

	return nil
}

// Send sends a message to the STOMP server as part of a transaction. The server will not process the
// message until the transaction is committed.
// This method returns without confirming that the STOMP server has received the message. If the STOMP server
// does fail to receive the message for any reason, the connection will close.
//
// The content type should be specified, according to the STOMP specification, but if contentType is an empty
// string, the message will be delivered without a content type header entry. The body array contains the
// message body, and its content should be consistent with the specified content type.
//
// TODO: document opts
func (tx *Transaction) Send(destination, contentType string, body []byte, opts ...func(*frame.Frame) error) error {
	if tx.completed {
		return ErrCompletedTransaction
	}

	f, err := createSendFrame(destination, contentType, body, opts)
	if err != nil {
		return err
	}

	f.Header.Set(frame.Transaction, tx.id)
	return tx.conn.sendFrame(f)
}

// Ack sends an acknowledgement for the message to the server. The STOMP
// server will not process the acknowledgement until the transaction
// has been committed. If the subscription has an AckMode of AckAuto, calling
// this function has no effect.
func (tx *Transaction) Ack(msg *Message) error {
	if tx.completed {
		return ErrCompletedTransaction
	}

	f, err := tx.conn.createAckNackFrame(msg, true)
	if err != nil {
		return err
	}

	if f != nil {
		f.Header.Set(frame.Transaction, tx.id)
		err := tx.conn.sendFrame(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Nack sends a negative acknowledgement for the message to the server,
// indicating that this client cannot or will not process the message and
// that it should be processed elsewhere. The STOMP server will not process
// the negative acknowledgement until the transaction has been committed.
// It is an error to call this method if the subscription has an AckMode
// of AckAuto, because the STOMP server will not be expecting any kind
// of acknowledgement (positive or negative) for this message.
func (tx *Transaction) Nack(msg *Message) error {
	if tx.completed {
		return ErrCompletedTransaction
	}

	f, err := tx.conn.createAckNackFrame(msg, false)
	if err != nil {
		return err
	}

	if f != nil {
		f.Header.Set(frame.Transaction, tx.id)
		err := tx.conn.sendFrame(f)
		if err != nil {
			return err
		}
	}

	return nil
}
