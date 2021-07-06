// logger for logger
package loglog

import (
	"os"
	"fmt"
)

// Error reports error encountered in the logger itself
var Error = func(err error) {
	fmt.Fprintf(os.Stderr, "countlog encountered error: %v\n", err)
	os.Stderr.Sync()
}

// Message can be set like this loglog.Message = fmt.Println
var Message = func(a ...interface{}) (n int, err error) {
	return 0, nil
}
