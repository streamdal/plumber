package awssns

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

// DisplayMessage returns nothing because SNS is a publish only backend
func (a *AWSSNS) DisplayMessage(_ *records.ReadRecord) error {
	return nil
}

func (a *AWSSNS) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}
