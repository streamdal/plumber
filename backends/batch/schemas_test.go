package batch

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestListSchemas_empty(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `[]`)

	output, err := b.listSchemas()
	g.Expect(err).To(Equal(errNoSchemas))
	g.Expect(len(output)).To(Equal(0))
}

func TestListSchemas_bad_response(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `{}`)

	output, err := b.listSchemas()
	g.Expect(err).To(Equal(errSchemaFailed))
	g.Expect(len(output)).To(Equal(0))
}

func TestListSchemas(t *testing.T) {
	g := NewGomegaWithT(t)

	apiResponse := `[{
	  "id": "44da12e6-6dfd-4f11-a952-6863958acf05",
	  "name": "Test Schema",
	  "type": "json",
	  "root_type": "",
	  "archived": false
	}]`

	b := BatchWithMockResponse(200, apiResponse)

	output, err := b.listSchemas()
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(len(output)).To(Equal(1))
	g.Expect(output[0].ID).To(Equal("44da12e6-6dfd-4f11-a952-6863958acf05"))
	g.Expect(output[0].Name).To(Equal("Test Schema"))
	g.Expect(output[0].Type).To(Equal("json"))
	g.Expect(output[0].RootType).To(Equal(""))
	g.Expect(output[0].Archived).To(BeFalse())
}
