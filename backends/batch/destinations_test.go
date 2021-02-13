package batch

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestListDestinations_empty(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `[]`)

	output, err := b.listDestinations()
	g.Expect(err).To(Equal(errNoDestinations))
	g.Expect(len(output)).To(Equal(0))
}

func TestListDestinations_bad_response(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `{}`)

	output, err := b.listDestinations()
	g.Expect(err).To(Equal(errDestinationsFailed))
	g.Expect(len(output)).To(Equal(0))
}

func TestListDestinations(t *testing.T) {
	g := NewGomegaWithT(t)

	apiResponse := `[{
		"id": "5803fb0e-f26a-498f-a6e3-c89c6230e147",
		"name": "Test Destination",
		"notes": "",
		"type": "kafka",
		"team_id": "90dd117d-ee04-4e05-8bee-17b691843737",
		"archived": true,
		"metadata": {
		  "topic": "test",
		  "address": "127.0.0.1",
		  "use_tls": true,
		  "password": "test",
		  "username": "test",
		  "sasl_type": "plain",
		  "insecure_tls": true
		},
		"author": {
		  "id": "c1c6c497-8594-4ae8-84ca-616172a007b3",
		  "name": "Mark Gregan",
		  "email": "mark@batch.sh"
		},
		"inserted_at": "2021-02-13T01:35:42.807576Z",
		"updated_at": "2021-02-13T01:35:42.807576Z"
	  }
	]`

	b := BatchWithMockResponse(200, apiResponse)

	output, err := b.listDestinations()
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(len(output)).To(Equal(1))
	g.Expect(output[0].ID).To(Equal("5803fb0e-f26a-498f-a6e3-c89c6230e147"))
	g.Expect(output[0].Name).To(Equal("Test Destination"))
	g.Expect(output[0].Type).To(Equal("kafka"))
	g.Expect(output[0].Archived).To(BeTrue())
}
