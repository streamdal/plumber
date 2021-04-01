package batch

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

	"github.com/kataras/tablewriter"
	"github.com/lensesio/tableprinter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

const (
	ApiUrl = "https://api.batch.sh"
)

type IBatch interface {
	LoadConfig()
	Login() error
	Logout() error
	ListReplays() error
	ListCollections() error
	ListSchemas() error
	ListDestinations() error
	Get(path string, queryParams map[string]string) ([]byte, int, error)
	Post(path string, params map[string]interface{}) ([]byte, int, error)
}

type Batch struct {
	Token   string
	TeamID  string
	UserID  string
	Log     *logrus.Entry
	Opts    *cli.Options
	Client  *http.Client
	Printer PrinterFunc
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

var errNotAuthenticated = errors.New("you are not authenticated. run `plumber batch login`")

// New creates a new instance of a Batch struct with defaults
func New(opts *cli.Options) *Batch {
	b := &Batch{
		Log:     logrus.WithField("pkg", "batch"),
		Opts:    opts,
		Client:  &http.Client{},
		Printer: printTable,
	}

	b.LoadConfig()

	return b
}

// getCookieJar builds a cookiejar, containing auth_token, to be used with http.Client
func (b *Batch) getCookieJar(path string) *cookiejar.Jar {
	cookies := make([]*http.Cookie, 0)

	u, _ := url.Parse(ApiUrl + path)

	if b.Token != "" {
		cookies = append(cookies, &http.Cookie{
			Name:   "auth_token",
			Value:  b.Token,
			Path:   "/",
			Domain: ".batch.sh",
		})
	}

	j, _ := cookiejar.New(nil)
	j.SetCookies(u, cookies)

	return j
}

// Post makes a GET request to the Batch.sh API
func (b *Batch) Get(path string, queryParams map[string]string) ([]byte, int, error) {

	if b.Client.Jar == nil {
		b.Client.Jar = b.getCookieJar(path)
	}

	params := url.Values{}
	if len(queryParams) > 0 {
		for k, v := range queryParams {
			params.Add(k, v)
		}
	}

	req, err := http.NewRequest(http.MethodGet, ApiUrl+path, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}

	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}

	defer resp.Body.Close()

	// Advise user to use `plumber batch login` first
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, 0, errNotAuthenticated
	}

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return contents, resp.StatusCode, nil
}

// Post makes a POST request to the Batch.sh API
func (b *Batch) Post(path string, params map[string]interface{}) ([]byte, int, error) {
	if b.Client.Jar == nil {
		b.Client.Jar = b.getCookieJar(path)
	}

	body, err := json.Marshal(params)
	if err != nil {
		return nil, 0, errors.Wrap(err, "bad parameters supplied")
	}

	req, err := http.NewRequest(http.MethodPost, ApiUrl+path, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}
	defer resp.Body.Close()

	// Advise user to use `plumber batch login` first
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
			b.Token = cookie.Value
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
