package pgoutput

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
)

const (
	pgDuplicateObjectErrorCode = "42710"
	pgOutputPlugin             = "pgoutput"
)

type Subscription struct {
	SlotName      string
	Publication   string
	WaitTimeout   time.Duration
	StatusTimeout time.Duration

	conn       *pgx.ReplicationConn
	maxWal     uint64
	walRetain  uint64
	walFlushed uint64

	failOnHandler bool

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func(Message, uint64) error

func NewSubscription(conn *pgx.ReplicationConn, slotName, publication string, walRetain uint64, failOnHandler bool) *Subscription {
	return &Subscription{
		SlotName:      slotName,
		Publication:   publication,
		WaitTimeout:   1 * time.Second,
		StatusTimeout: 10 * time.Second,

		conn:          conn,
		walRetain:     walRetain,
		failOnHandler: failOnHandler,
	}
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, publication)
}

// CreateSlot creates a replication slot if it doesn't exist
func (s *Subscription) CreateSlot() (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	if err = s.conn.CreateReplicationSlot(s.SlotName, pgOutputPlugin); err != nil {
		pgerr, ok := err.(pgx.PgError)
		if !ok || pgerr.Code != pgDuplicateObjectErrorCode {
			return
		}

		err = nil
	}

	return
}

func (s *Subscription) sendStatus(walWrite, walFlush uint64) error {
	if walFlush > walWrite {
		return fmt.Errorf("walWrite should be >= walFlush")
	}

	s.Lock()
	defer s.Unlock()

	k, err := pgx.NewStandbyStatus(walFlush, walFlush, walWrite)
	if err != nil {
		return fmt.Errorf("error creating status: %s", err)
	}

	if err = s.conn.SendStandbyStatus(k); err != nil {
		return err
	}

	return nil
}

// Flush sends the status message to server indicating that we've fully applied all of the events until maxWal.
// This allows PostgreSQL to purge it's WAL logs
func (s *Subscription) Flush() error {
	wp := atomic.LoadUint64(&s.maxWal)
	err := s.sendStatus(wp, wp)
	if err == nil {
		atomic.StoreUint64(&s.walFlushed, wp)
	}

	return err
}

func (s *Subscription) AdvanceLSN(lsn uint64) error {
	atomic.StoreUint64(&s.maxWal, lsn)

	return s.Flush()
}

// Start replication and block until error or ctx is canceled
func (s *Subscription) Start(ctx context.Context, startLSN uint64, h Handler) (err error) {
	err = s.conn.StartReplication(s.SlotName, startLSN, -1, pluginArgs("1", s.Publication))
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
	}

	s.maxWal = startLSN

	sendStatus := func() error {
		walWrite := atomic.LoadUint64(&s.maxWal)
		walLastFlushed := atomic.LoadUint64(&s.walFlushed)

		// Confirm only walRetain bytes in past
		// If walRetain is zero - will confirm current walPos as flushed
		walFlush := walWrite - s.walRetain

		if walLastFlushed > walFlush {
			// If there was a manual flush - report it's position until we're past it
			walFlush = walLastFlushed
		} else if walFlush < 0 {
			// If we have less than walRetain bytes - just report zero
			walFlush = 0
		}

		return s.sendStatus(walWrite, walFlush)
	}

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = sendStatus(); err != nil {
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Send final status and exit
			if err = sendStatus(); err != nil {
				return fmt.Errorf("unable to send final status: %s", err)
			}

			return

		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			s.Lock()
			message, err = s.conn.WaitForReplicationMessage(wctx)
			s.Unlock()
			cancel()

			if err == context.DeadlineExceeded {
				continue
			} else if err == context.Canceled {
				return
			} else if err != nil {
				return fmt.Errorf("replication failed: %s", err)
			}

			if message == nil {
				continue
				//return fmt.Errorf("replication failed: nil message received, should not happen")
			}

			if message.WalMessage != nil {
				var logmsg Message
				walStart := message.WalMessage.WalStart

				// Skip stuff that's in the past
				if walStart > 0 && walStart <= startLSN {
					continue
				}

				logmsg, err = Parse(message.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid message: %s", err)
				}

				// Ignore the error from handler for now
				if err = h(logmsg, walStart); err != nil && s.failOnHandler {
					return
				}
			} else if message.ServerHeartbeat != nil {
				if message.ServerHeartbeat.ReplyRequested == 1 {
					if err = sendStatus(); err != nil {
						return
					}
				}
			} else {
				return fmt.Errorf("no WalMessage/ServerHeartbeat defined in packet, should not happen")
			}
		}
	}
}
