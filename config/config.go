package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/server/types"
)

type IConfig interface {
	ConfigExists(fileName string) bool
	ReadConfig(fileName string) (*Config, error)
	WriteConfig(fileName string, data []byte) error
}

// Config stores Account IDs and the auth_token cookie
type Config struct {
	PlumberID   string                       `json:"plumber_id"`
	Token       string                       `json:"token"`
	TeamID      string                       `json:"team_id"`
	UserID      string                       `json:"user_id"`
	Connections map[string]*types.Connection `json:"connections"`
	Relays      map[string]*types.Relay      `json:"relays"`
	Schemas     map[string]*types.Schema     `json:"schemas"`
	GitHubToken string                       `json:"github_bearer_token"`
}

// Save is a convenience method of persisting the config to disk via a single call
func (p *Config) Save() error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config to JSON")
	}

	return WriteConfig("config.json", data)
}

// ReadConfig reads a config JSON file into a Config struct
func ReadConfig(fileName string) (*Config, error) {
	f, err := getConfigJson(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read ~/.batchsh/%s", fileName)
	}

	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read ~/.batchsh/%s", fileName)
	}

	cfg := &Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, errors.Wrapf(err, "could not unmarshal ~/.batchsh/%s", fileName)
	}

	if cfg.Connections == nil {
		cfg.Connections = make(map[string]*types.Connection)
	}
	if cfg.Relays == nil {
		cfg.Relays = make(map[string]*types.Relay)
	}
	if cfg.Schemas == nil {
		cfg.Schemas = make(map[string]*types.Schema)
	}

	logrus.Infof("Loaded '%d' stored connections", len(cfg.Connections))
	logrus.Infof("Loaded '%d' stored relays", len(cfg.Relays))

	return cfg, nil
}

// Exists determines if a config file exists yet
func Exists(fileName string) bool {
	configDir, err := getConfigDir()
	if err != nil {
		return false
	}
	configPath := path.Join(configDir, fileName)

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return false
	}

	return true
}

// WriteConfig writes a Batch struct as JSON into a config.json file
func WriteConfig(fileName string, data []byte) error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	configPath := path.Join(configDir, fileName)

	f, err := os.OpenFile(configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	return err
}

// getConfigJson attempts to read a user's .batchsh/config.json file to get saved credentials
func getConfigJson(fileName string) (*os.File, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return nil, err
	}
	configPath := path.Join(configDir, fileName)

	// Directory ~/.batchsh/ doesn't exist, create it
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := createConfigDir(); err != nil {
			return nil, err
		}

		// Create ~/.batchsh/config.json
		f, err := os.Create(path.Join(configDir, fileName))
		if err != nil {
			return nil, err
		}

		f.WriteString("{}")
	}

	// Config exists, open it
	return os.Open(configPath)
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
