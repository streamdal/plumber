package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/types"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
)

func (k *Kafka) DisplayMessage(msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	var err error

	// We either display consumer group lag stats or display the parsed record
	if _, ok := msg.Metadata["lag"]; ok {
		err = k.displayLag(msg)
	} else {
		err = k.displayRecord(msg)
	}

	return err
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	return nil
}

func (k *Kafka) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func (k *Kafka) displayRecord(msg *records.ReadRecord) error {
	record := msg.GetKafka()
	if record == nil {
		return errors.New("BUG: record in message is nil")
	}

	key := aurora.Gray(12, "NONE").String()

	if len(record.Key) != 0 {
		key = string(record.Key)
	}

	properties := [][]string{
		{"Key", key},
		{"topic", record.Topic},
		{"Offset", fmt.Sprintf("%d", record.Offset)},
		{"Partition", fmt.Sprintf("%d", record.Partition)},
	}

	// Display offset info if it exists
	if lastOffset, ok := msg.Metadata["last_offset"]; ok {
		lastOffsetInt, err := strconv.ParseInt(lastOffset, 10, 64)

		if err != nil {
			k.log.Errorf("unable to parse last_offset '%s': %s", lastOffset, err)
		} else {
			lastOffStr := strconv.FormatUint(uint64(lastOffsetInt), 10)
			properties = append(properties, []string{"LastOffset", lastOffStr})
		}
	}

	properties = append(properties, generateHeaders(record.Headers)...)

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(properties, msg.Num, receivedAt, msg.Payload)

	return nil
}

func generateHeaders(headers []*records.KafkaHeader) [][]string {
	if len(headers) == 0 {
		return [][]string{
			{"Header(s)", aurora.Gray(12, "NONE").String()},
		}
	}

	result := make([][]string, len(headers))
	result[0] = []string{
		"Header(s)", fmt.Sprintf("KEY: %s / VALUE: %s", headers[0].Key, headers[0].Value),
	}

	for i := 1; i != len(headers); i++ {
		result[i] = []string{
			"", fmt.Sprintf("KEY: %s / VALUE: %s", headers[i].Key, headers[i].Value),
		}
	}

	return result
}

func (k *Kafka) displayLag(msg *records.ReadRecord) error {
	if _, ok := msg.Metadata["lag"]; !ok {
		return errors.New("record doesn't appear to be a lag record (missing metadata)")
	}

	topicStats := make([]*types.TopicStats, 0)

	if err := json.Unmarshal(msg.XRaw, &topicStats); err != nil {
		return errors.Wrap(err, "unable to unmarshal raw lag data to topic stats")
	}

	properties := make([][]string, 0)

	for _, v := range topicStats {
		if len(properties) != 0 {
			// Add a separator between diff topics
			properties = append(properties, []string{
				"---------------------------------------------------------------",
			})
		}

		properties = append(properties, []string{
			"Topic", v.TopicName,
		}, []string{
			"Group ID", v.GroupID,
		})

		for partitionID, pstats := range v.Partitions {
			properties = append(properties, []string{
				"Partition ID", fmt.Sprint(partitionID),
			}, []string{
				"  └─ Messages Behind", fmt.Sprint(pstats.MessagesBehind),
			})
		}
	}

	printer.PrintTableProperties(properties, time.Unix(msg.ReceivedAtUnixTsUtc, 0))

	return nil
}
