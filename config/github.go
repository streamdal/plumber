package config

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/batchcorp/plumber/kv"

	"github.com/pkg/errors"
)

const GithubWASMReleaseURL = "https://api.github.com/repos/streamdal/dataqual-wasm/releases/latest"

// GithubReleaseResponse is the response from Github when calling /releases/latest endpoint
// We only need the download url
type GithubReleaseResponse struct {
	Assets []struct {
		URL string `json:"browser_download_url"`
	}
}

// BootstrapWASMFiles is called when a data quality rule is added to ensure we have WASM files available
func (c *Config) BootstrapWASMFiles() error {
	if c.haveDefaultWASMFiles() {
		return nil // already have default WASM files
	}

	c.log.Debug("Bootstrapping WASM files")

	if err := c.pullLatestWASMRelease(context.Background(), http.DefaultClient); err != nil {
		return errors.Wrap(err, "unable to pull latest WASM release")
	}

	return nil
}

// PullLatestWASMRelease pulls the latest WASM release from Github and stores it in KV
func (c *Config) pullLatestWASMRelease(ctx context.Context, client *http.Client) error {
	if client == nil {
		return errors.New("BUG: client is nil")
	}

	release, err := c.getWASMReleaseURL(ctx, client)
	if err != nil {
		return errors.Wrap(err, "unable to get WASM release URL")
	}

	c.log.Debugf("Downloading WAMS release files: %s", release.Assets[0].URL)

	if len(release.Assets) == 0 {
		return errors.New("no WASM assets found")
	}

	zip, err := c.downloadWASMZip(ctx, client, release.Assets[0].URL)
	if err != nil {
		return errors.Wrap(err, "unable to download WASM zip")
	}

	if err := c.storeWasmFiles(ctx, zip); err != nil {
		return errors.Wrap(err, "unable to store WASM files")
	}

	return nil
}

// getWASMReleaseURL gets the download URL for the latest WASM release from github
func (c *Config) getWASMReleaseURL(ctx context.Context, client *http.Client) (*GithubReleaseResponse, error) {
	if client == nil {
		return nil, errors.New("BUG: client is nil")
	}

	req, err := http.NewRequest("GET", GithubWASMReleaseURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get Github response")
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("non-200 response from Github")
	}

	release := &GithubReleaseResponse{}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read Github response body")
	}

	if err := json.Unmarshal(data, release); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal Github response")
	}

	return release, nil
}

// downloadWASMZip downloads the WASM zip from Github
func (c *Config) downloadWASMZip(ctx context.Context, client *http.Client, url string) (*zip.Reader, error) {
	if client == nil {
		return nil, errors.New("BUG: client is nil")
	}

	if url == "" {
		return nil, errors.New("url is empty")
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get Github response")
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("non-200 response from Github")
	}

	zipFile, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new zip reader")
	}

	return zipFile, nil
}

// storeWasmFiles unzips and stores the WASM files in KV
func (c *Config) storeWasmFiles(ctx context.Context, zipFile *zip.Reader) error {
	if c.KV == nil {
		return errors.New("BUG: kv store is nil")
	}

	// Unzip WASM files
	for _, file := range zipFile.File {
		// Just in case
		if file.FileInfo().IsDir() {
			continue
		}

		// Only want .wasm files
		if !strings.HasSuffix(file.Name, ".wasm") {
			continue
		}

		f, err := file.Open()
		if err != nil {
			return errors.Wrap(err, "unable to open file")
		}

		data, err := io.ReadAll(f)
		if err != nil {
			return errors.Wrap(err, "unable to read file")
		}

		_ = data

		if err := c.KV.Put(ctx, kv.WasmBucket, file.Name, data); err != nil {
			return errors.Wrap(err, "unable to store file")
		}

		c.log.Debugf("Stored WASM file '%s' in NATS", file.Name)
	}

	return nil
}

// haveDefaultWASMFiles checks if we have the default WASM files in KV.
// If the files are not present, plumber will fetch them from github
func (c *Config) haveDefaultWASMFiles() bool {
	c.WasmFilesMutex.RLock()
	defer c.WasmFilesMutex.RUnlock()

	// We have them in memory
	if len(c.WasmFiles) > 0 {
		return true
	}

	// Check KV
	keys, err := c.KV.Keys(context.Background(), kv.WasmBucket)
	if err != nil {
		return false
	}

	for _, v := range keys {
		if v == "match.wasm" {
			return true
		}
	}

	return false
}
