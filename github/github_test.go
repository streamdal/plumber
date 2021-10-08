package github

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var logger = &logrus.Logger{Out: ioutil.Discard}

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

//newHttpClient returns *http.Client with Transport replaced to avoid making real calls
func newHttpClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func NewWithMockResponse(httpCode int, responseBody string) *Github {
	client := newHttpClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: httpCode,
			Body:       ioutil.NopCloser(bytes.NewBufferString(responseBody)),
		}
	})

	return &Github{
		log:    logrus.NewEntry(logger),
		Client: client,
	}
}

var _ = Describe("Github", func() {
	Context("New", func() {
		It("returns configured struct", func() {
			g, err := New()
			Expect(err).ToNot(HaveOccurred())
			Expect(g.Client).To(BeAssignableToTypeOf(&http.Client{}))
			Expect(g.log).To(BeAssignableToTypeOf(&logrus.Entry{}))
		})
	})
	Context("Post", func() {
		It("returns response", func() {
			mockResp := `key=value&name=value`

			g := NewWithMockResponse(200, mockResp)
			res, code, err := g.Post("https://api.github.com", url.Values{})

			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(200))
			Expect(res).To(Equal([]byte(mockResp)))
		})
	})

	Context("parseRepoURL", func() {
		It("returns repo owner and name", func() {
			owner, name, err := parseRepoURL("https://www.github.com/batchcorp/collector-schemas")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(name).To(Equal("collector-schemas"))
		})

		It("handles full paths", func() {
			owner, name, err := parseRepoURL("https://www.github.com/batchcorp/collector-schemas/some/file.proto")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(name).To(Equal("collector-schemas"))
		})
	})

	Context("parseRepoFileURL", func() {
		It("succeeds", func() {
			owner, repo, filepath, err := parseRepoFileURL("https://www.github.com/batchcorp/collector-schemas/some/file.json")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(repo).To(Equal("collector-schemas"))
			Expect(filepath).To(Equal("some/file.json"))
		})
	})

	Context("getRepoArchiveLink", func() {
		// TODO
	})
})
