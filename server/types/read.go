package types

import (
	"context"
	"fmt"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/pb"
)

var (
	ErrMissingReadOptions = errors.New("read options cannot be nil")
	ErrMissingPlumberID   = errors.New("plumber id cannot be empty")
)

type AttachedStream struct {
	MessageCh chan *records.ReadRecord
}

type Read struct {
	AttachedClientsMutex *sync.RWMutex
	AttachedClients      map[string]*AttachedStream
	PlumberID            string
	ReadOptions          *opts.ReadOptions
	ContextCxl           context.Context
	CancelFunc           context.CancelFunc
	Backend              backends.Backend
	MessageDescriptors   map[pb.MDType]*desc.MessageDescriptor
	Log                  *logrus.Entry
}

type ReadConfig struct {
	ReadOptions *opts.ReadOptions
	PlumberID   string
	Backend     backends.Backend
}

func NewRead(cfg *ReadConfig) (*Read, error) {
	if err := validateReadConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "BUG: unable to validate read config")
	}

	ctx, cxl := context.WithCancel(context.Background())

	mds, err := createMessageDescriptors(cfg.ReadOptions)
	if err != nil {
		return nil, err
	}

	r := &Read{
		AttachedClientsMutex: &sync.RWMutex{},
		AttachedClients:      make(map[string]*AttachedStream),
		PlumberID:            cfg.PlumberID,
		ReadOptions:          cfg.ReadOptions,
		ContextCxl:           ctx,
		CancelFunc:           cxl,
		Backend:              cfg.Backend, // Will be filled in by StartRead()
		MessageDescriptors:   mds,
		Log:                  logrus.WithField("read_id", cfg.ReadOptions.XId),
	}

	return r, nil
}

func validateReadConfig(cfg *ReadConfig) error {
	if cfg.ReadOptions == nil {
		return ErrMissingReadOptions
	}

	if cfg.PlumberID == "" {
		return ErrMissingPlumberID
	}

	// Backend is intentionally not checked here because it may be set after a read is instantiated
	// since reads are not immediately started when they are created

	return nil
}

func createMessageDescriptors(readOpts *opts.ReadOptions) (map[pb.MDType]*desc.MessageDescriptor, error) {
	mds := make(map[pb.MDType]*desc.MessageDescriptor)

	if readOpts.DecodeOptions == nil {
		return mds, nil
	}

	if readOpts.DecodeOptions.DecodeType != encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		return mds, nil
	}

	pbSettings := readOpts.DecodeOptions.ProtobufSettings

	envelopeMD, err := pb.GetMDFromDescriptorBlob(pbSettings.XMessageDescriptor, pbSettings.ProtobufRootMessage)
	if err != nil {
		return nil, fmt.Errorf("unable to create read '%s': unable to generate "+
			"protobuf envelope message descriptor: %s", readOpts.XId, err)
	}

	mds[pb.MDEnvelope] = envelopeMD

	if pbSettings.ProtobufEnvelopeType == encoding.EnvelopeType_ENVELOPE_TYPE_SHALLOW {
		payloadMD, err := pb.GetMDFromDescriptorBlob(pbSettings.XShallowEnvelopeMessageDescriptor, pbSettings.ShallowEnvelopeMessage)
		if err != nil {
			return nil, fmt.Errorf("unable to create read '%s': unable to generate "+
				"protobuf envelope payload descriptor: %s", readOpts.XId, err)
		}

		mds[pb.MDPayload] = payloadMD
	}

	return mds, nil
}
