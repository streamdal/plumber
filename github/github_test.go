package github

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

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

	Context("parseRepoUrl", func() {
		It("returns repo owner and name", func() {
			owner, name, err := parseRepoUrl("https://www.github.com/batchcorp/collector-schemas")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(name).To(Equal("collector-schemas"))
		})

		It("handles full paths", func() {
			owner, name, err := parseRepoUrl("https://www.github.com/batchcorp/collector-schemas/some/file.proto")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(name).To(Equal("collector-schemas"))
		})
	})

	Context("GetUserCode", func() {
		It("returns an error on non-200 response", func() {
			g := NewWithMockResponse(401, `{}`)

			_, err := g.GetUserCode()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("non-200 response"))
		})
		It("returns an error with invalid expires_in", func() {
			g := NewWithMockResponse(200, `expires_in=abc`)

			_, err := g.GetUserCode()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid duration"))
		})

		It("returns an error with invalid expires_in", func() {
			g := NewWithMockResponse(200, `expires_in=5&interval=abc`)

			_, err := g.GetUserCode()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid duration"))
		})

		It("succeeds", func() {
			mockResp := `expires_in=5&interval=5&device_code=foo&user_code=AAAA-1111&verification_uri=https://github.com`

			g := NewWithMockResponse(200, mockResp)

			resp, err := g.GetUserCode()
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.UserCode).To(Equal("AAAA-1111"))
			Expect(resp.DeviceCode).To(Equal("foo"))
			Expect(resp.VerificationURL).To(Equal("https://github.com"))
			Expect(resp.CheckInterval).To(Equal(time.Second * 5))

		})
	})

	Context("GetAccessToken", func() {
		It("returns an error on non-200 response", func() {
			g := NewWithMockResponse(401, `{}`)
			_, err := g.GetAccessToken(&UserCodeResponse{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("non-200 response"))
		})

		It("handles a pending response", func() {
			cfg := &UserCodeResponse{
				CheckInterval: time.Second * 5,
			}

			g := NewWithMockResponse(200, `error=authorization_pending`)

			resp, err := g.GetAccessToken(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.SleepDuration).To(Equal(cfg.CheckInterval))
		})

		It("handles a slowdown response", func() {
			cfg := &UserCodeResponse{
				CheckInterval: time.Second * 5,
			}

			g := NewWithMockResponse(200, `error=slow_down&interval=10`)

			resp, err := g.GetAccessToken(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.SleepDuration).To(Equal(time.Second * 10))
		})

		It("returns the bearer token", func() {
			expectedToken := "gfsdf708gff9dsd98"
			cfg := &UserCodeResponse{
				CheckInterval: time.Second * 5,
			}

			g := NewWithMockResponse(200, "access_token="+expectedToken)

			resp, err := g.GetAccessToken(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.BearerToken).To(Equal(expectedToken))
		})
	})

	Context("PollForAccessToken", func() {
		It("times out at the given deadline", func() {
			cfg := &UserCodeResponse{
				ExpiresIn: time.Now().UTC().Add(time.Millisecond * 100),
			}

			g := NewWithMockResponse(200, `error=authorization_pending`)

			_, err := g.PollForAccessToken(cfg)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrVerifyTimeout))
		})

		It("returns the access token", func() {
			expectedToken := "gfsdf708gff9dsd98"

			cfg := &UserCodeResponse{
				ExpiresIn: time.Now().UTC().Add(time.Second * 5),
			}

			g := NewWithMockResponse(200, "access_token="+expectedToken)

			token, err := g.PollForAccessToken(cfg)
			Expect(err).ToNot(HaveOccurred())
			Expect(token).To(Equal(expectedToken))
		})
	})

	Context("getRepoArchiveLink", func() {
		// TODO
	})
})
