package batch

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestListReplays_empty(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `[]`)

	output, err := b.listReplays()
	g.Expect(err).To(Equal(errNoReplays))
	g.Expect(len(output)).To(Equal(0))
}

func TestListReplays_bad_response(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `{}`)

	output, err := b.listReplays()
	g.Expect(err).To(Equal(errReplaysFailed))
	g.Expect(len(output)).To(Equal(0))
}

func TestListReplays(t *testing.T) {
	g := NewGomegaWithT(t)

	apiResponse := `[{
	  "id": "44da12e6-6dfd-4f11-a952-6863958acf05",
	  "name": "Test Replay",
	  "type": "kafka",
	  "query": "*",
	  "paused": false,
      "collection": {"name": "Test Collection"},
      "destination": {"name": "Test Destination"}
	}]`

	b := BatchWithMockResponse(200, apiResponse)

	output, err := b.listReplays()
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(len(output)).To(Equal(1))
	g.Expect(output[0].ID).To(Equal("44da12e6-6dfd-4f11-a952-6863958acf05"))
	g.Expect(output[0].Name).To(Equal("Test Replay"))
	g.Expect(output[0].Type).To(Equal("kafka"))
	g.Expect(output[0].Query).To(Equal("*"))
	g.Expect(output[0].Paused).To(BeFalse())
	g.Expect(output[0].Collection).To(Equal("Test Collection"))
	g.Expect(output[0].Destination).To(Equal("Test Destination"))
}
