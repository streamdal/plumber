package batch

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"syscall"

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
		Name string `json: "name"`
	}
}

// Login attempts to login to the Batch.sh API using credentials supplied via stdin
func (b *Batch) Login() error {

	// No credentials, or expired, ask for username/password
	username, err := readUsername()
	if err != nil {
		return err
	}

	password, err := readPassword()
	if err != nil {
		return err
	}

	authResponse, err := b.Authenticate(username, password)
	if err != nil {
		fmt.Println("failed login")
		return errors.Wrap(err, "could not authenticate")
	}

	cfg := &Config{
		TeamID: authResponse.Team.ID,
		UserID: authResponse.AccountID,
		Token:  b.Token,
	}

	// Successfully authenticated, write token to cache
	if err := writeConfig(cfg); err != nil {
		return errors.Wrap(err, "unable to cache login credentials")
	}

	b.Log.Info("Authentication successful!")

	return nil
}

func (b *Batch) Logout() error {
	// Perform APi logout

	// Clear saved credentials
	cfg, err := readConfig()
	if err != nil {
		// Just clearing these out for the sake of cleaning up. We don't need to worry about errors at this point
		return nil
	}

	cfg.Token = ""
	cfg.TeamID = ""
	cfg.UserID = ""

	writeConfig(cfg)

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
		return nil, errors.New("invalid login")
	}

	authResponse := &AuthResponse{}
	if err := json.Unmarshal(res, authResponse); err != nil {
		return nil, err
	}

	return authResponse, nil
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
