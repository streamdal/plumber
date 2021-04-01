package batch

import (
	"github.com/batchcorp/plumber/cli"
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

	Context("CreateDestination", func() {
		It("Creates a destination", func() {
			apiResponse := `{
				"id": "3e2c0a91-af49-4eee-a8db-9ae523682cfc",
				"name": "test http",
				"notes": "",
				"type": "http",
				"team_id": "fa2b3482-2a00-43ea-a0ed-341974a5f6ef",
				"archived": false,
				"metadata": {
					"url": "http://localhost",
					"headers": [
						{
							"content-type": "application/json"
						}
					]
				},
				"author": {
					"id": "1503e318-9c87-4e86-ab6c-fda235cb4bde",
					"name": "mark test",
					"email": "mark-new@batch.sh"
				},
				"inserted_at": "2021-04-01T15:15:00.835626Z",
				"updated_at": "2021-04-01T15:15:00.835626Z"
			}`

			b := BatchWithMockResponse(200, apiResponse)
			b.Opts.Batch.DestinationMetadata = &cli.DestinationMetadata{}

			destination, err := b.createDestination("http")
			Expect(err).ToNot(HaveOccurred())
			Expect(destination.ID).To(Equal("3e2c0a91-af49-4eee-a8db-9ae523682cfc"))
		})
	})

	Context("getDestinationMetadataHTTP", func() {
		It("returns correct map", func() {
			b := &Batch{
				Opts: &cli.Options{
					Batch: &cli.BatchOptions{
						DestinationMetadata: &cli.DestinationMetadata{
							HTTPURL: "http://localhost",
							HTTPHeaders: map[string]string{
								"content-type": "application/json",
							},
						},
					},
				},
			}

			md := b.getDestinationMetadataHTTP()

			// Return is map[string]interface{}. We need to type assert the "headers" key to test
			headers, ok := md["headers"].([]map[string]string)
			Expect(ok).To(BeTrue())

			Expect(md["url"]).To(Equal("http://localhost"))
			Expect(headers[0]).To(Equal(map[string]string{"content-type": "application/json"}))
		})
	})
})
