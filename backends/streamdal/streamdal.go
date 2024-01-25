// Package batch is used for interacting with the Streamdal platform's API. This
// backend is a non-traditional backend and does not implement the Backend
// interface; it should be used independently.
package streamdal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/config"

	"github.com/kataras/tablewriter"
	"github.com/lensesio/tableprinter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultAPIURL = "https://api.streamdal.com"
)

type IBatch interface {
	LoadConfig()
	Login() error
	Logout() error
	ListReplays() error
	ListCollections() error
	ListSchemas() error
	ListDestinations() error
	Get(path string, queryParams map[string]string) (content []byte, statusCode int, err error)
	Post(path string, params map[string]interface{}) (content []byte, statusCode int, err error)
	Delete(path string) (content []byte, statusCode int, err error)
}

type Streamdal struct {
	PersistentConfig *config.Config
	Log              *logrus.Entry
	Opts             *opts.CLIOptions
	Client           *http.Client
	Printer          PrinterFunc
	ApiUrl           string
}

type BlunderError struct {
	Code     int    `json:"code"`
	Domain   string `json:"domain"`
	Field    string `json:"field"`
	Status   string `json:"status"`
	RawError string `json:"raw_error"`
	Message  string `json:"message"`
}

type BlunderErrorResponse struct {
	Errors []*BlunderError `json:"errors"`
}

// PrinterFunc is a function that will be used to display output to the user's console
type PrinterFunc func(v interface{})

var errNotAuthenticated = errors.New("you are not authenticated. run `plumber streamdal login`")

// New creates a new instance of a Streamdal struct with defaults
func New(cliOpts *opts.CLIOptions, cfg *config.Config) *Streamdal {
	printer := printTable

	if cliOpts.Streamdal.OutputType == opts.StreamdalOutputType_JSON {
		printer = printJSON
	}

	b := &Streamdal{
		PersistentConfig: cfg,
		Log:              logrus.WithField("pkg", "batch"),
		Opts:             cliOpts,
		Client:           &http.Client{},
		Printer:          printer,
		ApiUrl:           cliOpts.Streamdal.ApiUrl,
	}

	if b.ApiUrl == "" {
		b.ApiUrl = DefaultAPIURL
	}

	return b
}

// getCookieJar builds a cookiejar, containing auth_token, to be used with http.Client
func (b *Streamdal) getCookieJar(path string) *cookiejar.Jar {
	cookies := make([]*http.Cookie, 0)

	u, _ := url.Parse(b.ApiUrl + path)

	if b.PersistentConfig.Token != "" {
		cookies = append(cookies, &http.Cookie{
			Name:   "auth_token",
			Value:  b.PersistentConfig.Token,
			Path:   "/",
			Domain: "." + u.Hostname(),
		})
	}

	j, _ := cookiejar.New(nil)
	j.SetCookies(u, cookies)

	return j
}

// Get makes a GET request to the Streamdal API
func (b *Streamdal) Get(path string, queryParams map[string]string) ([]byte, int, error) {

	if b.Client.Jar == nil {
		b.Client.Jar = b.getCookieJar(path)
	}

	params := url.Values{}
	if len(queryParams) > 0 {
		for k, v := range queryParams {
			params.Add(k, v)
		}
	}

	req, err := http.NewRequest(http.MethodGet, b.ApiUrl+path, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}

	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}

	defer resp.Body.Close()

	// Advise user to use `plumber streamdal login` first
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, 0, errNotAuthenticated
	}

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return contents, resp.StatusCode, nil
}

// Post makes a POST request to the Streamdal API
func (b *Streamdal) Post(path string, params map[string]interface{}) ([]byte, int, error) {
	if b.Client.Jar == nil {
		b.Client.Jar = b.getCookieJar(path)
	}

	body, err := json.Marshal(params)
	if err != nil {
		return nil, 0, errors.Wrap(err, "bad parameters supplied")
	}

	req, err := http.NewRequest(http.MethodPost, b.ApiUrl+path, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, errors.New("unable to create new request for post")
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}
	defer resp.Body.Close()

	// Advise user to use `plumber streamdal login` first
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, 0, errNotAuthenticated
	}

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	// Save auth_token cookie value
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "auth_token" {
			b.PersistentConfig.Token = cookie.Value
		}
	}

	return contents, resp.StatusCode, nil
}

func (b *Streamdal) Delete(path string) ([]byte, int, error) {
	if b.Client.Jar == nil {
		b.Client.Jar = b.getCookieJar(path)
	}

	req, err := http.NewRequest(http.MethodDelete, b.ApiUrl+path, nil)
	if err != nil {
		return nil, 0, errors.New("unable to create new request for delete")
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}
	defer resp.Body.Close()

	// Advise user to use `plumber streamdal login` first
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, 0, errNotAuthenticated
	}

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	// Save auth_token cookie value
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "auth_token" {
			b.PersistentConfig.Token = cookie.Value
		}
	}

	return contents, resp.StatusCode, nil
}

// printTable displays a slice of structs in an ASCII table
func printTable(v interface{}) {
	printer := tableprinter.New(os.Stdout)

	printer.HeaderAlignment = tableprinter.AlignCenter
	printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
	printer.CenterSeparator = "│"
	printer.ColumnSeparator = "│"
	printer.RowSeparator = "─"
	printer.HeaderBgColor = tablewriter.BgBlackColor
	printer.HeaderFgColor = tablewriter.FgCyanColor

	printer.Print(v)
}

// printJSON displays output from batch commands as JSON. Needed for automation purposes
func printJSON(v interface{}) {
	output, err := json.Marshal(v)
	if err != nil {
		fmt.Printf(`{"error": "%s"}\n`, err.Error())
	}

	fmt.Println(string(output))
}
