package batch

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/batchcorp/plumber/config"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
)

// AuthResponse is used to unmarshal the JSON results of a login API call
type AuthResponse struct {
	AccountID             string `json:"id"`
	Name                  string `json:"name"`
	Email                 string `json:"email"`
	OnboardingState       string `json:"onboarding_state"`
	OnboardingStateStatus string `json:"onboarding_state_status"`
	Team                  struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
}

var (
	errCouldNotLogin   = errors.New("could not authenticate")
	errMissingUsername = errors.New("you must enter a username")
	errMissingPassword = errors.New("you must enter a password")
	errMaxTries        = errors.New("maximum number of retries exceeded")
)

// Login attempts to login to the Batch.sh API using credentials supplied via stdin
func (b *Batch) Login() error {

	// No credentials, or expired, ask for username/password
	username, err := readUsername(os.Stdin)
	if err != nil {
		return err
	}

	password, err := readPassword(readPasswordFromTerminal)
	if err != nil {
		return err
	}

	authResponse, err := b.Authenticate(username, password)
	if err != nil {
		return errCouldNotLogin
	}

	cfg := &config.Config{
		TeamID: authResponse.Team.ID,
		UserID: authResponse.AccountID,
		Token:  b.PersistentConfig.Token,
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config data")
	}

	// Successfully authenticated, write token to cache
	if err := config.WriteConfig("config.json", data); err != nil {
		return errors.Wrap(err, "unable to cache login credentials")
	}

	b.Log.Info("Authentication successful!")

	return nil
}

// Logout logs a user out of the Batch.sh API and clears saved credentials
func (b *Batch) Logout() error {
	// Perform APi logout
	b.Post("/auth/logout", nil)

	// Clear saved credentials
	cfg, err := config.ReadConfig("config.json")
	if err != nil {
		// Just clearing these out for the sake of cleaning up. We don't need to worry about errors at this point
		return nil
	}

	cfg.Token = ""
	cfg.TeamID = ""
	cfg.UserID = ""

	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config data")
	}

	config.WriteConfig("config.json", data)

	return nil
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
		return nil, errCouldNotLogin
	}

	authResponse := &AuthResponse{}
	if err := json.Unmarshal(res, authResponse); err != nil {
		return nil, err
	}

	return authResponse, nil
}

// readUsername reads a password from stdin
func readUsername(stdin io.Reader) (string, error) {
	for {
		fmt.Print("\n\nEnter Username: ")

		// int typecast is needed for windows
		reader := bufio.NewReader(stdin)
		username, err := reader.ReadString('\n')
		if err != nil {
			return "", errMissingUsername
		}

		s := strings.TrimSpace(username)
		if s != "" {
			return s, nil
		}
	}
}

func readPasswordFromTerminal(fd int) ([]byte, error) {
	return terminal.ReadPassword(fd)
}

// readPassword securely reads a password from stdin
func readPassword(readPassword func(fd int) ([]byte, error)) (string, error) {
	for {
		fmt.Print("Enter Password: ")

		// int typecast is needed for windows
		password, err := readPassword(int(syscall.Stdin))
		if err != nil {
			return "", errMissingPassword
		}

		fmt.Println("")

		sp := strings.TrimSpace(string(password))
		if sp != "" {
			return sp, nil
		}
	}
}
