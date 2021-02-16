package batch

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Batch", func() {
	Context("listReplays", func() {
		It("returns an error when empty", func() {
			b := BatchWithMockResponse(200, `[]`)

			output, err := b.listReplays()
			Expect(err).To(Equal(errNoReplays))
			Expect(len(output)).To(Equal(0))
		})

		It("returns error on a bad response", func() {
			b := BatchWithMockResponse(200, `{}`)

			output, err := b.listReplays()
			Expect(err).To(Equal(errReplaysFailed))
			Expect(len(output)).To(Equal(0))
		})

		It("lists replays", func() {
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
			Expect(err).ToNot(HaveOccurred())
			Expect(len(output)).To(Equal(1))
			Expect(output[0].ID).To(Equal("44da12e6-6dfd-4f11-a952-6863958acf05"))
			Expect(output[0].Name).To(Equal("Test Replay"))
			Expect(output[0].Type).To(Equal("kafka"))
			Expect(output[0].Query).To(Equal("*"))
			Expect(output[0].Paused).To(BeFalse())
			Expect(output[0].Collection).To(Equal("Test Collection"))
			Expect(output[0].Destination).To(Equal("Test Destination"))
		})
	})
})
