package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos"

	"github.com/google/go-github/v37/github"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

const (
	// RepoListMaxResults is the number of results to return when getting a repository list
	// GitHub hard-limits this to 100
	RepoListMaxResults = 100
)

var (
	ErrVerifyTimeout = errors.New("Unable to verify GitHub access after 15 minutes")
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IGithub
type IGithub interface {
	GetRepoArchive(ctx context.Context, token, owner, repo string) ([]byte, error)
	GetRepoFile(ctx context.Context, token, owner, repo, sha string) ([]byte, error)
	GetRepoList(ctx context.Context, installID int64, token string) ([]string, error)
	GetRepoTree(ctx context.Context, token, owner, repo string) (*github.Tree, error)
	Post(url string, values url.Values) ([]byte, int, error)
}

type Github struct {
	Client *http.Client
	log    *logrus.Entry
}

type AccessTokenResponse struct {
	BearerToken   string
	SleepDuration time.Duration
}

// RepoListResponse is used to unmarshal a response from api.github.com/user/installations/.../repositories
// The GitHub SDK does not have a method for that endpoint for some odd reason
type RepoListResponse struct {
	Repositories []*github.Repository `json:"repositories"`
}

// New returns a configured Github struct
func New() (*Github, error) {
	return &Github{
		Client: &http.Client{},
		log:    logrus.WithField("pkg", "github/github_handlers.go"),
	}, nil
}

// Get makes a GET request with an oAuth token and returns the resulting body contents
func (g *Github) Get(url, oAuthToken string) ([]byte, int, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Authorization", "token "+oAuthToken)

	resp, err := g.Client.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	return contents, resp.StatusCode, nil
}

// Post makes a form post and returns the resulting body contents
func (g *Github) Post(url string, values url.Values) ([]byte, int, error) {
	resp, err := g.Client.PostForm(url, values)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", url, err)
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return contents, resp.StatusCode, nil
}

// GetRepoArchive returns a zip of the contents of a github repository
func (g *Github) GetRepoArchive(ctx context.Context, token, owner, repo string) ([]byte, error) {
	archiveURL, err := getRepoArchiveLink(ctx, token, owner, repo)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repo)
	}

	// Download contents
	resp, err := g.Client.Get(archiveURL.String())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repo)
	}

	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repo)
	}

	return contents, nil
}

// GetRepoFile gets a single file from github. Used for Avro and JSONSchema imports
func (g *Github) GetRepoFile(ctx context.Context, token, owner, repo, sha string) ([]byte, error) {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	blob, _, err := client.Git.GetBlob(ctx, owner, repo, sha)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get file contents")
	}

	if blob.GetEncoding() != "base64" {
		return nil, fmt.Errorf("BUG: unable to handle file encoding '%s'", blob.GetEncoding())
	}

	return base64.StdEncoding.DecodeString(blob.GetContent())
}

// GetRepoList gets all repositories that an install has access to
func (g *Github) GetRepoList(ctx context.Context, installID int64, token string) ([]string, error) {
	repos := make([]string, 0)

	// Results are paged, with a maximum of 100 per page
	page := 1

	for {
		url := fmt.Sprintf(
			"https://api.github.com/user/installations/%d/repositories?per_page=%d&page=%d",
			installID,
			RepoListMaxResults,
			page,
		)

		repoJSON, _, err := g.Get(url, token)
		if err != nil {
			return nil, err
		}

		repoResponse := &RepoListResponse{}
		if err := json.Unmarshal(repoJSON, repoResponse); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal github API response")
		}

		for _, repo := range repoResponse.Repositories {
			repos = append(repos, repo.GetHTMLURL())
		}

		// No more pages of results since we have less results than the limit
		if len(repoResponse.Repositories) < RepoListMaxResults {
			break
		}

		page++
	}

	return repos, nil
}

func (g *Github) GetRepoTree(ctx context.Context, token, owner, repo string) (*github.Tree, error) {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	repoInfo, _, err := client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get repository info")
	}

	mainBranch, _, err := client.Repositories.GetBranch(ctx, owner, repo, repoInfo.GetDefaultBranch(), false)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get main branch info")
	}

	tree, _, err := client.Git.GetTree(ctx, owner, repo, mainBranch.Commit.GetSHA(), true)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get repository file listing")
	}

	return tree, nil
}

// parseRepoFileURL parses a URL pointing to a specific file within a repository
func parseRepoFileURL(in string) (owner, repo, filepath string, err error) {
	parts, err := url.Parse(in)
	if err != nil {
		return "", "", "", errors.Wrap(err, "unable to parse GitHub repository URL")
	}
	names := strings.Split(strings.TrimLeft(parts.Path, "/"), "/")
	return names[0], names[1], strings.Join(names[4:], "/"), nil
}

// parseRepoURL extracts the repo name and owner name from a github URL
func parseRepoURL(in string) (string, string, error) {
	parts, err := url.Parse(in)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to parse GitHub repository URL")
	}
	names := strings.Split(strings.TrimLeft(parts.Path, "/"), "/")
	return names[0], names[1], nil
}

// getRepoArchiveLink queries github's API for the link to download a zip of the repository
func getRepoArchiveLink(ctx context.Context, token, owner, repo string) (*url.URL, error) {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	tc := oauth2.NewClient(ctx, ts)

	client := github.NewClient(tc)

	archiveURL, _, err := client.Repositories.GetArchiveLink(ctx, owner, repo, github.Zipball, nil, true)
	if err != nil {
		return nil, err
	}

	return archiveURL, nil
}

// TreeToDisplay turns a github GetRepoTree response into a nested structure
// This is necessary because GitHub's API returns a flat list of files
func TreeToDisplay(entries []*github.TreeEntry, schemaType protos.SchemaType) *protos.Directory {
	rootDir := &protos.Directory{
		Name:     "/",
		FullPath: "/",
		Files:    make([]*protos.File, 0),
		Dirs:     make(map[string]*protos.Directory),
	}

	for _, entry := range entries {
		if entry.GetType() != "blob" {
			// Don't care about dirs, we infer them from the blob path
			continue
		}

		// Have to start at root dir for every file
		cwd := rootDir

		path := filepath.Dir(entry.GetPath())
		fileName := filepath.Base(entry.GetPath())

		if skipFile(schemaType, fileName) {
			continue
		}

		// We're at root path, add to root rather than trying to navigate down a path
		if path == "." {
			cwd.Files = append(cwd.Files, &protos.File{
				Name: fileName,
				Path: entry.GetPath(),
				Sha:  entry.GetSHA(),
				Size: int64(entry.GetSize()),
			})
			continue
		}

		parts := strings.Split(path, "/")

		// Navigate down the file path, check if we have a map entry for each directory name
		for _, subDir := range parts {
			found, ok := cwd.Dirs[subDir]
			if ok {
				// Already initialized this path
				cwd = found
			} else {
				// Initialize a new subDir under the current working directory
				cwd.Dirs[subDir] = &protos.Directory{
					Name:     subDir,
					FullPath: path,
					Files:    make([]*protos.File, 0),
					Dirs:     make(map[string]*protos.Directory),
				}
				cwd = cwd.Dirs[subDir]
			}
		}

		cwd.Files = append(cwd.Files, &protos.File{
			Name: fileName,
			Path: entry.GetPath(),
			Sha:  entry.GetSHA(),
			Size: int64(entry.GetSize()),
		})

	}

	return rootDir
}

func skipFile(schemaType protos.SchemaType, fileName string) bool {
	if schemaType == protos.SchemaType_SCHEMA_TYPE_PROTOBUF && !strings.HasSuffix(fileName, ".proto") {
		return true
	}

	if schemaType == protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA && !strings.HasSuffix(fileName, ".json") {
		return true
	}

	if schemaType == protos.SchemaType_SCHEMA_TYPE_AVRO && !strings.HasSuffix(fileName, ".avsc") {
		return true
	}

	return false
}
