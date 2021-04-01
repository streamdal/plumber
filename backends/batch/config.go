package batch

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
)

// Config stores Account IDs and the auth_token cookie
type Config struct {
	Token  string `json:"token"`
	TeamID string `json:"team_id"`
	UserID string `json:"user_id"`
}

// Attempt to load stored credentials
func (b *Batch) LoadConfig() {
	// Load config
	cfg, err := readConfig()
	if err != nil {
		return
	}

	b.TeamID = cfg.TeamID
	b.UserID = cfg.UserID
	b.Token = cfg.Token
}

// readConfig reads a user's .batchsh/config.json into a Batch struct
func readConfig() (*Config, error) {
	f, err := getConfigJson()
	if err != nil {
		return nil, errors.Wrap(err, "could not read ~/.batchsh/config.json")
	}

	defer f.Close()

	configJson, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "could not read ~/.batchsh/config.json")
	}

	cfg := &Config{}
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
func writeConfig(cfg *Config) error {
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
	return err
}

// getConfigDir returns a directory where the batch configuration will be stored
func getConfigDir() (string, error) {
	// Get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "unable to locate user's home directory")
	}

	return path.Join(homeDir, ".batchsh"), nil
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
