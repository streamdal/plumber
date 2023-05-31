package config

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/kv"
	"github.com/batchcorp/plumber/server/types"
)

const (
	// GithubWASMReleaseURL points to the repo containing the compiled default function .wasm modules
	GithubWASMReleaseURL = "https://api.github.com/repos/streamdal/dataqual-wasm/releases/latest"
)

// GithubReleaseResponse is the response from Github when calling /releases/latest endpoint
// We only need the download url
type GithubReleaseResponse struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		URL string `json:"browser_download_url"`
	}
}

// BootstrapWASMFiles is called when a data quality rule is added to ensure we have WASM files available
func (c *Config) BootstrapWASMFiles(ctx context.Context) error {
	if c.haveDefaultWASMFiles() {
		return nil // already have default WASM files
	}

	c.log.Debug("Bootstrapping WASM files")

	if err := c.pullLatestWASMRelease(ctx, http.DefaultClient); err != nil {
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

	if c.enableCluster {
		if err := c.storeWASMFilesKV(ctx, zip, release.TagName); err != nil {
			return errors.Wrap(err, "unable to store WASM files")
		}
	} else {
		if err := c.storeWASMFilesFS(ctx, zip, release.TagName); err != nil {
			return errors.Wrap(err, "unable to store WASM files")
		}
	}

	return nil
}

// getWASMReleaseURL gets the download URL for the latest WASM release from github
func (c *Config) getWASMReleaseURL(ctx context.Context, client *http.Client) (*GithubReleaseResponse, error) {
	if client == nil {
		return nil, errors.New("BUG: client is nil")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", GithubWASMReleaseURL, nil)
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

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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
func (c *Config) storeWASMFilesKV(ctx context.Context, zipFile *zip.Reader, version string) error {
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

		c.log.Debugf("Stored WASM file '%s' in KV", file.Name)
		c.SetWasmFile(file.Name, &types.WasmFile{Name: file.Name, Version: version})
	}

	return nil
}

func (c *Config) storeWASMFilesFS(_ context.Context, zipFile *zip.Reader, version string) error {
	if zipFile == nil {
		return errors.New("BUG: zipFile is nil")
	}

	configDir, err := getConfigDir()
	if err != nil {
		return errors.Wrap(err, "unable to get config dir")
	}

	// Create dir if needed
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		if err := os.Mkdir(configDir, 0700); err != nil {
			c.log.Errorf("unable to create config directory '%s': %v", configDir, err)

			return errors.Wrapf(err, "unable to create config directory %s", configDir)
		}
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

		configDir, err := getConfigDir()
		if err != nil {
			return errors.New("unable to get config directory")
		}

		filePath := path.Join(configDir, file.Name)

		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return errors.Wrapf(err, "unable to write '%s'", filePath)
		}

		c.log.Debugf("Stored WASM file '%s' in '%s'", file.Name, configDir)
		c.SetWasmFile(file.Name, &types.WasmFile{Name: file.Name, Version: version})
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

	if c.enableCluster {
		// Cluster mode, check KV
		return c.haveDefaultWASMFilesKV()
	} else {
		// Standalone mode, check file system
		return c.haveDefaultWASMFilesFS()
	}

	return false
}

func (c *Config) haveDefaultWASMFilesKV() bool {
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

func (c *Config) haveDefaultWASMFilesFS() bool {
	configDir, err := getConfigDir()
	if err != nil {
		return false
	}

	if _, err := os.Stat(path.Join(configDir, "match.wasm")); err == nil {
		return true
	}

	return false
}
