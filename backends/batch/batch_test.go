package batch

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

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
		Transport: RoundTripFunc(fn),
	}
}

func BatchWithMockResponse(httpCode int, responseBody string) *Batch {
	client := newHttpClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: httpCode,
			Body:       ioutil.NopCloser(bytes.NewBufferString(responseBody)),
		}
	})

	return &Batch{
		Log:    logrus.NewEntry(logger),
		Client: client,
	}
}

func TestGetCookieJar(t *testing.T) {
	g := NewGomegaWithT(t)

	const Token = "testin123"

	b := &Batch{
		Token: Token,
	}

	jar := b.getCookieJar("/v1/collection")

	u, _ := url.Parse(ApiUrl + "/v1/collection")
	cookies := jar.Cookies(u)

	g.Expect(len(cookies)).To(Equal(1))
	g.Expect(cookies[0].Name).To(Equal("auth_token"))
	g.Expect(cookies[0].Value).To(Equal(Token))
}
