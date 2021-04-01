package batch

import (
	"bytes"
	"testing"

	. "github.com/onsi/gomega"
)

func TestAuthenticate(t *testing.T) {
	g := NewGomegaWithT(t)

	apiResponse := `{
	  "id": "8d8af58b-7d3d-474f-82ff-8b228245d159",
	  "name": "Test User",
	  "email": "test@batch.sh",
	  "onboarding_state": "",
	  "onboarding_state_status": "",
	  "team": {
		"id": "dce9c35e-1762-4233-97b8-e3f1830faf57",
	    "name": "Testing-1"
	  }
	}`

	b := BatchWithMockResponse(200, apiResponse)

	output, err := b.Authenticate("test@batch.sh", "password123")
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(output).To(BeAssignableToTypeOf(&AuthResponse{}))
	g.Expect(output.AccountID).To(Equal("8d8af58b-7d3d-474f-82ff-8b228245d159"))
	g.Expect(output.Name).To(Equal("Test User"))
	g.Expect(output.Email).To(Equal("test@batch.sh"))
	g.Expect(output.Team.ID).To(Equal("dce9c35e-1762-4233-97b8-e3f1830faf57"))
	g.Expect(output.Team.Name).To(Equal("Testing-1"))
}

func TestReadUsername(t *testing.T) {
	g := NewGomegaWithT(t)

	var stdin bytes.Buffer
	stdin.Write([]byte("test@batch.sh\n"))

	username, err := readUsername(&stdin)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(username).To(Equal("test@batch.sh"))
}

func TestReadPassword(t *testing.T) {
	g := NewGomegaWithT(t)

	var stdin bytes.Buffer
	stdin.Write([]byte("test@batch.sh\n"))

	var testfunc = func(fd int) ([]byte, error) {
		return []byte("solarwinds123"), nil
	}

	username, err := readPassword(testfunc)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(username).To(Equal("solarwinds123"))
}
