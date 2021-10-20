package github

import (
	"bytes"
	"context"
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
			owner, repo, filepath, err := parseRepoFileURL("https://www.github.com/batchcorp/collector-schemas/blob/master/some/file.json")
			Expect(err).ToNot(HaveOccurred())
			Expect(owner).To(Equal("batchcorp"))
			Expect(repo).To(Equal("collector-schemas"))
			Expect(filepath).To(Equal("some/file.json"))
		})
	})

	Context("getRepoArchiveLink", func() {
		// TODO
	})

	Context("GetRepoList", func() {
		It("returns list of repositories for an install", func() {
			mockResp := `{
    "total_count": 1,
    "repositories": [
        {
            "id": 1234567,
            "node_id": "MFEw0lJlcG1zaXRvc3kzMDQ6NDkzMjK=",
            "name": "testrepo",
            "full_name": "batchcorp/testrepo",
            "private": true,
            "owner": {
                "login": "batchcorp",
                "id": 123456,
                "avatar_url": "https://avatars.githubusercontent.com/u/28496506?v=4",
                "gravatar_id": "",
                "url": "https://api.github.com/users/batchcorp",
                "html_url": "https://github.com/batchcorp",
                "followers_url": "https://api.github.com/users/batchcorp/followers",
                "following_url": "https://api.github.com/users/batchcorp/following{/other_user}",
                "gists_url": "https://api.github.com/users/batchcorp/gists{/gist_id}",
                "starred_url": "https://api.github.com/users/batchcorp/starred{/owner}{/repo}",
                "subscriptions_url": "https://api.github.com/users/batchcorp/subscriptions",
                "organizations_url": "https://api.github.com/users/batchcorp/orgs",
                "repos_url": "https://api.github.com/users/batchcorp/repos",
                "events_url": "https://api.github.com/users/batchcorp/events{/privacy}",
                "received_events_url": "https://api.github.com/users/batchcorp/received_events",
                "type": "Organization",
                "site_admin": false
            },
            "html_url": "https://github.com/batchcorp/testrepo"
        }
    ]
}`
			g := NewWithMockResponse(200, mockResp)
			repos, err := g.GetRepoList(context.Background(), 1234, "oauth_token")

			Expect(err).ToNot(HaveOccurred())
			Expect(repos).To(Equal([]string{"https://github.com/batchcorp/testrepo"}))
		})
	})
})
