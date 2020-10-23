package activemq

import (
	"testing"

	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/cli"
)

func TestGetDestination_topic(t *testing.T) {
	g := NewGomegaWithT(t)

	opts := &cli.Options{
		ActiveMq: &cli.ActiveMqOptions{
			Topic: "test_topic",
		},
	}

	a := &ActiveMq{
		Options: opts,
	}

	got := a.getDestination()
	g.Expect(got).To(Equal("/topic/test_topic"))
}

func TestGetDestination_queue(t *testing.T) {
	g := NewGomegaWithT(t)

	opts := &cli.Options{
		ActiveMq: &cli.ActiveMqOptions{
			Queue: "TestQueue",
		},
	}

	a := &ActiveMq{
		Options: opts,
	}

	got := a.getDestination()
	g.Expect(got).To(Equal("TestQueue"))
}
