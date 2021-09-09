package activemq

import (
	"testing"

	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/options"
)

func TestGetDestination_topic(t *testing.T) {
	g := NewGomegaWithT(t)

	opts := &options.Options{
		ActiveMq: &options.ActiveMqOptions{
			Topic: "test_topic",
		},
	}

	a := &ActiveMq{
		ConnectionConfig: opts,
	}

	got := a.getDestination()
	g.Expect(got).To(Equal("/topic/test_topic"))
}

func TestGetDestination_queue(t *testing.T) {
	g := NewGomegaWithT(t)

	opts := &options.Options{
		ActiveMq: &options.ActiveMqOptions{
			Queue: "TestQueue",
		},
	}

	a := &ActiveMq{
		ConnectionConfig: opts,
	}

	got := a.getDestination()
	g.Expect(got).To(Equal("TestQueue"))
}
