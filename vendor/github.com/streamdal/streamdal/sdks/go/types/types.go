package types

import "github.com/streamdal/streamdal/libs/protos/build/go/protos"

type CounterName string

const (
	ConsumeBytes          CounterName = "counter_consume_bytes"
	ProduceBytes          CounterName = "counter_produce_bytes"
	ConsumeProcessedCount CounterName = "counter_consume_processed"
	ProduceProcessedCount CounterName = "counter_produce_processed"
	ConsumeErrorCount     CounterName = "counter_consume_errors"
	ProduceErrorCount     CounterName = "counter_produce_errors"
	ConsumeBytesRate      CounterName = "counter_consume_bytes_rate"
	ProduceBytesRate      CounterName = "counter_produce_bytes_rate"
	ConsumeProcessedRate  CounterName = "counter_consume_processed_rate"
	ProduceProcessedRate  CounterName = "counter_produce_processed_rate"
	NotifyCount           CounterName = "counter_notify"
	DroppedTailMessages   CounterName = "counter_dropped_tail_messages"
)

type CounterEntry struct {
	Name     CounterName // counter name
	Audience *protos.Audience
	Labels   map[string]string
	Value    int64
}
