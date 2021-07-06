package msgfmt

import (
	"fmt"
	"time"
	"strings"
)

var Formats = map[string]Format{
	"goTime": &GoTimeFormat{},
}

type Format interface {
	FormatterOf(targetKey string, formatArgs []string, sample []interface{}) (Formatter, error)
}

func newFuncFormatter(key string, funcName string, funcArgs []string, sample []interface{}) (ret Formatter, err error) {
	defer func() {
		recovered := recover()
		if recovered != nil {
			err = fmt.Errorf("newFormatter panic: %v", recovered)
		}
	}()
	format := Formats[funcName]
	if format == nil {
		return nil, fmt.Errorf("format %s is not supported", funcName)
	}
	return format.FormatterOf(key, funcArgs, sample)
}

type GoTimeFormat struct {
}

func (format *GoTimeFormat) FormatterOf(targetKey string, formatArgs []string, sample []interface{}) (Formatter, error) {
	if len(formatArgs) == 0 {
		formatArgs = append(formatArgs, time.RFC3339)
	}
	for i := 0; i < len(sample); i+= 2{
		key := sample[i].(string)
		if key == targetKey {
			_, isTime := sample[i+1].(time.Time)
			if !isTime {
				return nil, fmt.Errorf("%s is not time.Time", targetKey)
			}
			layout := strings.TrimSpace(formatArgs[0])
			return &goTimeFormatter{
				idx: i+1,
				layout: layout,
			}, nil
		}
	}
	return nil, fmt.Errorf("%s not found in properties", targetKey)
}

type goTimeFormatter struct {
	idx    int
	layout string
}

func (formatter *goTimeFormatter) Format(space []byte, kv []interface{}) []byte {
	return kv[formatter.idx].(time.Time).AppendFormat(space, formatter.layout)
}
