package validate

import (
	"github.com/pkg/errors"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"
	"github.com/streamdal/streamdal/libs/protos/build/go/protos/shared"
)

var (
	ErrNilInput = errors.New("request cannot be nil")
)

func ErrEmptyField(field string) error {
	return errors.Errorf("field '%s' cannot be empty", field)
}

func ErrNilField(field string) error {
	return errors.Errorf("field '%s' cannot be nil", field)
}

func ErrUnsetEnum(field string) error {
	return errors.Errorf("enum '%s' cannot be unset", field)
}

func Audience(aud *protos.Audience) error {
	if aud == nil {
		return ErrNilInput
	}

	if aud.ServiceName == "" {
		return ErrEmptyField("service_name")
	}

	if aud.ComponentName == "" {
		return ErrEmptyField("component_name")
	}

	if aud.OperationName == "" {
		return ErrEmptyField("operation_name")
	}

	if aud.OperationType == protos.OperationType_OPERATION_TYPE_UNSET {
		return ErrUnsetEnum("operation_type")
	}

	return nil
}

func SetPipelinesCommand(cmd *protos.Command) error {
	if cmd == nil {
		return ErrNilInput
	}

	if cmd.GetSetPipelines() == nil {
		return errors.New("not a SetPipelines command")
	}

	return nil
}

func KVInstruction(i *protos.KVInstruction) error {
	if i == nil {
		return errors.New("KVInstruction cannot be nil")
	}

	if i.Action == shared.KVAction_KV_ACTION_UNSET {
		return errors.New("KVAction cannot be UNSET")
	}

	if i.Action != shared.KVAction_KV_ACTION_DELETE_ALL && i.Object == nil {
		return errors.New("KVInstruction.Object cannot be nil")
	}

	return nil
}

func TailRequestStartCommand(cmd *protos.Command) error {
	if cmd == nil {
		return ErrNilInput
	}

	tail := cmd.GetTail()
	if tail == nil {
		return ErrNilField("Tail")
	}

	req := tail.GetRequest()
	if req == nil {
		return ErrNilField("Request")
	}

	if req.Id == "" {
		return ErrEmptyField("Id")
	}

	if err := Audience(req.Audience); err != nil {
		return errors.Wrap(err, "invalid audience")
	}

	return nil
}

func TailRequestStopCommand(cmd *protos.Command) error {
	if cmd == nil {
		return ErrNilInput
	}

	tail := cmd.GetTail()
	if tail == nil {
		return ErrNilField("Tail")
	}

	req := tail.GetRequest()
	if req == nil {
		return ErrNilField("Request")
	}

	if req.Id == "" {
		return ErrEmptyField("Id")
	}

	if err := Audience(req.Audience); err != nil {
		return errors.Wrap(err, "invalid audience")
	}

	return nil
}

func TailRequestPauseCommand(cmd *protos.Command) error {
	if cmd == nil {
		return ErrNilInput
	}

	tail := cmd.GetTail()
	if tail == nil {
		return ErrNilField("Tail")
	}

	req := tail.GetRequest()
	if req == nil {
		return ErrNilField("Request")
	}

	if req.Id == "" {
		return ErrEmptyField("Id")
	}

	if err := Audience(req.Audience); err != nil {
		return errors.Wrap(err, "invalid audience")
	}

	return nil
}

func TailRequestResumeCommand(cmd *protos.Command) error {
	if cmd == nil {
		return ErrNilInput
	}

	tail := cmd.GetTail()
	if tail == nil {
		return ErrNilField("Tail")
	}

	req := tail.GetRequest()
	if req == nil {
		return ErrNilField("Request")
	}

	if req.Id == "" {
		return ErrEmptyField("Id")
	}

	if err := Audience(req.Audience); err != nil {
		return errors.Wrap(err, "invalid audience")
	}

	return nil
}

func KVCommand(kv *protos.KVCommand) error {
	if kv == nil {
		return ErrNilInput
	}

	return nil
}
