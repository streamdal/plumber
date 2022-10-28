package batch

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Batch", func() {
	Context("ListSchemas", func() {
		It("returns error when no schemas exist", func() {
			b := BatchWithMockResponse(200, `[]`)

			output, err := b.listSchemas()
			Expect(err).To(Equal(errNoSchemas))
			Expect(len(output)).To(Equal(0))
		})

		It("returns error on a bad response", func() {
			b := BatchWithMockResponse(200, `{}`)

			output, err := b.listSchemas()
			Expect(err).To(Equal(errSchemaFailed))
			Expect(len(output)).To(Equal(0))
		})

		It("returns list of schemas", func() {
			apiResponse := `[{
			  "id": "44da12e6-6dfd-4f11-a952-6863958acf05",
			  "name": "Test Schema",
			  "type": "json",
			  "root_type": "",
			  "archived": false
			}]`

			b := BatchWithMockResponse(200, apiResponse)

			output, err := b.listSchemas()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(output)).To(Equal(1))
			Expect(output[0].ID).To(Equal("44da12e6-6dfd-4f11-a952-6863958acf05"))
			Expect(output[0].Name).To(Equal("Test Schema"))
			Expect(output[0].Type).To(Equal("json"))
			Expect(output[0].Archived).To(BeFalse())
		})
	})
})
