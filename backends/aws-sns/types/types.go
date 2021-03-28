package types

import (
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . ISNSAPI
type ISNSAPI interface {
	snsiface.SNSAPI
}
