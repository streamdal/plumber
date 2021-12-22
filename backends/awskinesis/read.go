package awskinesis

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

// getShardIterator creates a shard interator based on read arguments
// lastSequenceNumber argument is used when creating anther iterator after a recoverable read failure
func (k *Kinesis) getShardIterator(args *args.AWSKinesisReadArgs, shardID string, lastSequenceNumber *string) (*string, error) {
	getOpts := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(args.Stream),
	}

	if args.ReadTrimHorizon {
		getOpts.ShardIteratorType = aws.String("TRIM_HORIZON")
	} else if args.ReadSequenceNumber != "" {
		getOpts.ShardIteratorType = aws.String("AT_SEQUENCE_NUMBER")
		getOpts.StartingSequenceNumber = aws.String(args.ReadSequenceNumber)
	} else if args.ReadFromTimestamp > 0 {
		getOpts.ShardIteratorType = aws.String("AT_TIMESTAMP")
		getOpts.Timestamp = aws.Time(time.Unix(int64(args.ReadFromTimestamp), 0))
	} else if args.ReadAfterSequenceNumber != "" {
		getOpts.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		getOpts.StartingSequenceNumber = aws.String(args.ReadAfterSequenceNumber)
	} else {
		getOpts.ShardIteratorType = aws.String("LATEST")
	}

	if lastSequenceNumber != nil {
		getOpts.StartingSequenceNumber = lastSequenceNumber
	}

	//Get shard iterator
	shardResp, err := k.client.GetShardIterator(getOpts)
	if err != nil {
		return nil, err
	}

	return shardResp.ShardIterator, nil
}

// getShards returns a slice of all shards that exist for a stream
// This method is used when no --stream argument is provided. In this case, we will
// start a read for each available shard
func (k *Kinesis) getShards(args *args.AWSKinesisReadArgs) ([]string, error) {
	shardResp, err := k.client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(args.Stream),
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to get shards")
	}

	out := make([]string, 0)

	for _, shard := range shardResp.StreamDescription.Shards {
		out = append(out, util.DerefString(shard.ShardId))
	}

	return out, nil
}

// Read reads records from a Kinesis stream
func (k *Kinesis) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	// Shared specofied by user, start a read for only that shard
	if readOpts.AwsKinesis.Args.Shard != "" {
		return k.readShard(ctx, readOpts.AwsKinesis.Args.Shard, readOpts, resultsChan, errorChan)
	}

	// No shard specified. Get all shards and launch a goroutine to read each shard
	shards, err := k.getShards(readOpts.AwsKinesis.Args)
	if err != nil {
		return errors.Wrap(err, "unable to get shards")
	}

	var wg sync.WaitGroup

	for _, shardID := range shards {
		wg.Add(1)

		go func(shardID string) {
			defer wg.Done()

			k.log.Debugf("Launching read for shard '%s'", shardID)

			if err := k.readShard(ctx, shardID, readOpts, resultsChan, errorChan); err != nil {
				util.WriteError(nil, errorChan, err)
			}
		}(shardID)
	}

	wg.Wait()

	return nil
}

// readShard reads from a single shard in a kinesis stream
func (k *Kinesis) readShard(ctx context.Context, shardID string, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	// In the event of a recoverable error, we want to start where we left off, if the user
	// is reading by specific sequence number. Otherwise this value will be ignored
	var lastSequenceNumber *string

	shardIterator, err := k.getShardIterator(readOpts.AwsKinesis.Args, shardID, lastSequenceNumber)
	if err != nil {
		return errors.Wrap(err, "unable to create shard iterator")
	}

	k.log.Infof("Waiting for messages for shard '%s'...", shardID)

	for {
		resp, err := k.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         aws.Int64(readOpts.AwsKinesis.Args.MaxRecords),
		})
		if err != nil {
			// Some errors are recoverable
			if !canRetry(err) {
				// Can't recover, exit out
				util.WriteError(nil, errorChan, err)
				return nil
			}

			// Log so we know it happened, but no need to send error record to the client if we can recover
			util.WriteError(k.log, nil, err)

			// Get new iterator. lastSequenceNumber is specified in the event that the user
			// started reading from a specific sequence number. We want to start where we left off
			shardIterator, err = k.getShardIterator(readOpts.AwsKinesis.Args, shardID, lastSequenceNumber)
			if err != nil {
				return errors.Wrap(err, "unable to create shard iterator")
			}

			continue
		}

		for _, msg := range resp.Records {
			serializedMsg, err := json.Marshal(msg)
			if err != nil {
				return errors.Wrap(err, "unable to serialize message into JSON")
			}

			// Count may be shared across multiple reads
			atomic.AddUint64(&k.readCount, 1)

			resultsChan <- k.genReadRecord(msg, serializedMsg, shardID)

			lastSequenceNumber = msg.SequenceNumber
		}

		if resp.NextShardIterator == nil || resp.NextShardIterator == shardIterator {
			k.log.Info("No more messages in shard, stopping read")
			break
		}

		// NextShardIterator is a pagination key, it needs to be specified so that kinesis
		// returns the next set of records, otherwise it will return the same records over and over
		shardIterator = resp.NextShardIterator

		if !readOpts.Continuous {
			break
		}

	}

	return nil
}

// genReadRecord returns a read record for a kinesis message.
// Keeping this in a separate method to keep Read() somewhat clean
func (k *Kinesis) genReadRecord(msg *kinesis.Record, serializedMsg []byte, shardID string) *records.ReadRecord {
	return &records.ReadRecord{
		MessageId:           uuid.NewV4().String(),
		Num:                 int64(k.readCount),
		ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
		Payload:             msg.Data,
		XRaw:                serializedMsg,
		Record: &records.ReadRecord_AwsKinesis{
			AwsKinesis: &records.AWSKinesis{
				PartitionKey:   util.DerefString(msg.PartitionKey),
				SequenceNumber: util.DerefString(msg.SequenceNumber),
				EncryptionType: util.DerefString(msg.EncryptionType),
				ShardId:        shardID,
				Value:          msg.Data,
			},
		},
	}
}

// canRetry determines if an error is recoverable
func canRetry(err error) bool {
	switch err.(type) {
	case *kinesis.ProvisionedThroughputExceededException:
		// Exceeded limits, we can retry
		return true
	}

	return false
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.AwsKinesis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.AwsKinesis.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Stream == "" {
		return ErrEmptyStream
	}

	if args.Shard == "" {
		// If no shard is specified, then we will read from all shards
		// However this won't work if reading via a sequence number, as those are unique to a shard
		if args.ReadSequenceNumber != "" || args.ReadAfterSequenceNumber != "" {
			return ErrEmptyShardWithSequence
		}
	}

	return nil
}
