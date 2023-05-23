package memphis

import (
	"errors"
	"strings"
)

func memphisError(err error) error {
	if err == nil {
		return nil
	}
	message := strings.Replace(err.Error(), "nats", "memphis", -1)
	return errors.New(message)
}
