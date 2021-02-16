package batch

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Batch", func() {
	Context("ListDestinations", func() {
		It("returns an error when empty", func() {
			b := BatchWithMockResponse(200, `[]`)

			output, err := b.listDestinations()
			Expect(err).To(Equal(errNoDestinations))
			Expect(len(output)).To(Equal(0))
		})

		It("returns error on a bad response", func() {
			b := BatchWithMockResponse(200, `{}`)

			output, err := b.listDestinations()
			Expect(err).To(Equal(errDestinationsFailed))
			Expect(len(output)).To(Equal(0))
		})

		It("lists destinations", func() {
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
			Expect(err).ToNot(HaveOccurred())
			Expect(len(output)).To(Equal(1))
			Expect(output[0].ID).To(Equal("5803fb0e-f26a-498f-a6e3-c89c6230e147"))
			Expect(output[0].Name).To(Equal("Test Destination"))
			Expect(output[0].Type).To(Equal("kafka"))
			Expect(output[0].Archived).To(BeTrue())
		})
	})
})
