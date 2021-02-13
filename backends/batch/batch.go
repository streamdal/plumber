package batch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/user"
	"path"
	"strings"
	"syscall"

	"github.com/kataras/tablewriter"
	"github.com/lensesio/tableprinter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/cli"
)

const (
	ApiUrl        = "https://api.dev.batch.sh"
	MaxLoginTries = 5
)

// Batch stores Account IDs and the auth_token cookie
type Batch struct {
	Token  string        `json:"token"`
	TeamID string        `json:"team_id"`
	UserID string        `json:"user_id"`
	log    *logrus.Entry `json:"-"`
	Opts   *cli.Options  `json:"-"`
}

// Try attempts to login to the API via saved credentials.
// If no credentials exist, the user will be prompted for their username/password
// If credentials exist, it will attempt to make an API call using them. If that fails, the user will be
// prompted for their username/password
func Try(opts *cli.Options) (*Batch, error) {
	// Load config
	b, err := readConfig()
	if err != nil {
		return Login(opts)
	}

	b.Opts = opts
	b.log = logrus.WithField("pkg", "batch")

	// Attempt to access API with saved credentials
	_, respCode, err := b.Get("/v1/team/member", nil)
	if respCode != http.StatusOK {
		return Login(opts)
	}

	// Successful authentication
	return b, nil
}

// New creates a new instance of a Batch struct with defaults
func New(opts *cli.Options) *Batch {
	return &Batch{
		log:  logrus.WithField("pkg", "batch"),
		Opts: opts,
	}
}

// Login attempts to login to the Batch.sh API using credentials supplied via stdin
func Login(opts *cli.Options) (*Batch, error) {
	b := New(opts)

	// No creds, or expired, ask for username/password
	username, err := readUsername()
	if err != nil {
		return nil, err
	}

	password, err := readPassword()
	if err != nil {
		return nil, err
	}

	authResponse, err := b.Authenticate(username, password)
	if err != nil {
		fmt.Println("failed login")
		return nil, errors.Wrap(err, "could not authenticate")
	}

	b.TeamID = authResponse.Team.ID
	b.UserID = authResponse.AccountID

	// Successfully authenticated, write token to cache
	if err := writeConfig(b); err != nil {
		return nil, errors.Wrap(err, "unable to cache login credentials")
	}

	return b, nil
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

	client := http.Client{
		Jar: b.getCookieJar(path),
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

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}

	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return contents, resp.StatusCode, nil
}

// Post makes a POST request to the Batch.sh API
func (b *Batch) Post(path string, params map[string]interface{}) ([]byte, int, error) {
	body, err := json.Marshal(params)
	if err != nil {
		return nil, 0, errors.Wrap(err, "bad parameters supplied")
	}

	client := http.Client{
		Jar: b.getCookieJar(path),
	}

	req, err := http.NewRequest(http.MethodPost, ApiUrl+path, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", path, err)
	}
	defer resp.Body.Close()

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

// AuthResponse is used to unmarshal the JSON results of a login API call
type AuthResponse struct {
	AccountID             string `json:"id"`
	Name                  string `json:"name"`
	Email                 string `json:"email"`
	OnboardingState       string `json:"onboarding_state"`
	OnboardingStateStatus string `json:"onboarding_state_status"`
	Team                  struct {
		ID   string `json:"id"`
		Name string `json: "name"`
	}
}

// Authenticate makes an API call to the Batch.sh API with the given account's credentials
func (b *Batch) Authenticate(username, password string) (*AuthResponse, error) {
	res, code, err := b.Post("/v1/login", map[string]interface{}{
		"email":    username,
		"password": password,
	})

	if err != nil {
		return nil, err
	}

	if code != http.StatusPermanentRedirect && code != http.StatusOK {
		return nil, errors.New("invalid login")
	}

	authResponse := &AuthResponse{}
	if err := json.Unmarshal(res, authResponse); err != nil {
		return nil, err
	}

	return authResponse, nil
}

// readConfig reads a user's .batchsh/config.json into a Batch struct
func readConfig() (*Batch, error) {
	f, err := getConfigJson()
	if err != nil {
		return nil, errors.Wrap(err, "could not read ~/.batchsh/config.json")
	}

	defer f.Close()

	configJson, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "could not read ~/.batchsh/config.json")
	}

	cfg := New(nil)

	if err := json.Unmarshal(configJson, cfg); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal ~/.batchsh/config.json")
	}

	return cfg, nil
}

// getConfigJson attempts to read a user's .batchsh/config.json file to get saved credentials
func getConfigJson() (*os.File, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return nil, err
	}
	configPath := path.Join(configDir, "config.json")

	// Directory ~/.batchsh/ doesn't exist, create it
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := createConfigDir(); err != nil {
			return nil, err
		}

		// Create ~/.batchsh/config.json
		f, err := os.Create(path.Join(configDir, "config.json"))
		if err != nil {
			return nil, err
		}

		f.WriteString("{}")
	}

	// Config exists, open it
	return os.Open(configPath)
}

// writeConfig writes a Batch struct as JSON into a config.json file
func writeConfig(cfg *Batch) error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	configPath := path.Join(configDir, "config.json")

	f, err := os.OpenFile(configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	fmt.Println("write file")
	return nil
}

// getConfigDir returns a directory where the batch configuration will be stored
func getConfigDir() (string, error) {
	// Get user's home directory
	usr, err := user.Current()
	if err != nil {
		return "", errors.Wrap(err, "unable to locate user's home directory")
	}

	return path.Join(usr.HomeDir, ".batchsh"), nil
}

// createConfigDir will create a json file located at ~/.batchsh/config.json to store plumber authentication credentials
func createConfigDir() error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	if err := os.Mkdir(configDir, 0755); err != nil && !os.IsExist(err) {
		return errors.Wrap(err, "unable to create .batchsh directory")
	}

	return nil
}

// readUsername reads a password from stdin
func readUsername() (string, error) {
	for {
		fmt.Print("\n\nEnter Username: ")

		// int typecast is needed for windows
		reader := bufio.NewReader(os.Stdin)
		username, err := reader.ReadString('\n')
		if err != nil {
			return "", errors.New("you must enter a username")
		}

		s := strings.TrimSpace(username)
		if s != "" {
			return s, nil
		}
	}
}

// readPassword securely reads a password from stdin
func readPassword() (string, error) {
	for i := 0; i < MaxLoginTries; i++ {
		fmt.Print("Enter Password: ")

		// int typecast is needed for windows
		password, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", errors.New("you must enter a password")
		}

		fmt.Println("")

		sp := strings.TrimSpace(string(password))
		if sp != "" {
			return sp, nil
		}
	}

	return "", errors.New("maximum number of retries exceeded")
}

// PrintTable displays a slice of structs in an ASCII table
func PrintTable(v interface{}) {
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
