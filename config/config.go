// Package config is used for storing and manipulating (server) state in plumber.
// There should be, at most, a single instance of the plumber config that is
// passed around between various components.
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	stypes "github.com/batchcorp/plumber/server/types"
)

type IConfig interface {
	ConfigExists(fileName string) bool
	ReadConfig(fileName string) (*Config, error)
	WriteConfig(fileName string, data []byte) error
}

// Config stores Account IDs and the auth_token cookie
type Config struct {
	PlumberID       string `json:"plumber_id"`
	Token           string `json:"token"`
	TeamID          string `json:"team_id"`
	UserID          string `json:"user_id"`
	VCServiceToken  string `json:"vc_service_token"`    // entire vc-service JWT
	GitHubToken     string `json:"github_bearer_token"` // retrieved from vc-service JWT contents
	GitHubInstallID int64  `json:"install_id"`

	Connections         map[string]*opts.ConnectionOptions     `json:"-"`
	Relays              map[string]*stypes.Relay               `json:"-"`
	Schemas             map[string]*protos.Schema              `json:"-"`
	Services            map[string]*protos.Service             `json:"-"`
	Reads               map[string]*stypes.Read                `json:"-"`
	ImportRequests      map[string]*protos.ImportGithubRequest `json:"-"`
	Validations         map[string]*common.Validation          `json:"-"`
	ConnectionsMutex    *sync.RWMutex                          `json:"-"`
	ServicesMutex       *sync.RWMutex                          `json:"-"`
	ReadsMutex          *sync.RWMutex                          `json:"-"`
	RelaysMutex         *sync.RWMutex                          `json:"-"`
	SchemasMutex        *sync.RWMutex                          `json:"-"`
	ImportRequestsMutex *sync.RWMutex                          `json:"-"`
	ValidationsMutex    *sync.RWMutex                          `json:"-"`
}

// Save is a convenience method of persisting the config to disk via a single call
func (c *Config) Save() error {
	tmpCfg := *c

	// Don't want to save these to a file, they live in etcd
	tmpCfg.GitHubToken = ""
	tmpCfg.VCServiceToken = ""
	tmpCfg.GitHubInstallID = 0

	data, err := json.Marshal(c)
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

	cfg := &Config{
		ConnectionsMutex:    &sync.RWMutex{},
		ServicesMutex:       &sync.RWMutex{},
		ReadsMutex:          &sync.RWMutex{},
		RelaysMutex:         &sync.RWMutex{},
		SchemasMutex:        &sync.RWMutex{},
		ImportRequestsMutex: &sync.RWMutex{},
		ValidationsMutex:    &sync.RWMutex{},
		Connections:         make(map[string]*opts.ConnectionOptions),
		Relays:              make(map[string]*stypes.Relay),
		Schemas:             make(map[string]*protos.Schema),
		Services:            make(map[string]*protos.Service),
		Reads:               make(map[string]*stypes.Read),
		ImportRequests:      make(map[string]*protos.ImportGithubRequest),
		Validations:         make(map[string]*common.Validation),
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, errors.Wrapf(err, "could not unmarshal ~/.batchsh/%s", fileName)
	}

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

	// ReadOptions exists, open it
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

// GetRead returns an in-progress read from the Read map
func (c *Config) GetRead(readID string) *stypes.Read {
	c.ReadsMutex.RLock()
	defer c.ReadsMutex.RUnlock()

	r, _ := c.Reads[readID]

	return r
}

// SetRead adds an in-progress read to the Read map
func (c *Config) SetRead(readID string, read *stypes.Read) {
	c.ReadsMutex.Lock()
	defer c.ReadsMutex.Unlock()

	c.Reads[readID] = read
}

// DeleteRead removes a read from in-memory map
func (c *Config) DeleteRead(readID string) {
	c.ReadsMutex.Lock()
	defer c.ReadsMutex.Unlock()
	delete(c.Reads, readID)
}

// GetRelay returns a relay from the in-memory map
func (c *Config) GetRelay(relayID string) *stypes.Relay {
	c.RelaysMutex.RLock()
	defer c.RelaysMutex.RUnlock()

	r, _ := c.Relays[relayID]

	return r
}

// SetRelay saves a relay to in-memory map
func (c *Config) SetRelay(relayID string, relay *stypes.Relay) {
	c.RelaysMutex.Lock()
	c.Relays[relayID] = relay
	c.RelaysMutex.Unlock()
}

// DeleteRelay removes a service from in-memory map
func (c *Config) DeleteRelay(relayID string) {
	c.RelaysMutex.Lock()
	defer c.RelaysMutex.Unlock()
	delete(c.Relays, relayID)
}

// SetService saves a service to in-memory map
func (c *Config) SetService(serviceID string, svc *protos.Service) {
	c.ServicesMutex.Lock()
	defer c.ServicesMutex.Unlock()
	c.Services[serviceID] = svc
}

// GetService returns a service from the in-memory map
func (c *Config) GetService(serviceID string) *protos.Service {
	c.ServicesMutex.RLock()
	defer c.ServicesMutex.RUnlock()

	r, _ := c.Services[serviceID]

	return r
}

// DeleteService removes a service from in-memory map
func (c *Config) DeleteService(schemaID string) {
	c.ServicesMutex.Lock()
	defer c.ServicesMutex.Unlock()
	delete(c.Services, schemaID)
}

// GetSchema returns a stored schema
func (c *Config) GetSchema(schemaID string) *protos.Schema {
	c.SchemasMutex.RLock()
	defer c.SchemasMutex.RUnlock()

	s, _ := c.Schemas[schemaID]

	return s
}

// SetSchema saves a schema to in-memory map
func (c *Config) SetSchema(schemaID string, schema *protos.Schema) {
	c.SchemasMutex.Lock()
	defer c.SchemasMutex.Unlock()
	c.Schemas[schemaID] = schema
}

// DeleteSchema removes a schema from in-memory map
func (c *Config) DeleteSchema(schemaID string) {
	c.SchemasMutex.Lock()
	defer c.SchemasMutex.Unlock()
	delete(c.Schemas, schemaID)
}

// GetConnection retrieves a connection from in-memory map
func (c *Config) GetConnection(connID string) *opts.ConnectionOptions {
	c.ConnectionsMutex.RLock()
	defer c.ConnectionsMutex.RUnlock()

	conn, _ := c.Connections[connID]

	return conn
}

// SetConnection saves a connection to in-memory map
func (c *Config) SetConnection(connID string, conn *opts.ConnectionOptions) {
	c.ConnectionsMutex.Lock()
	defer c.ConnectionsMutex.Unlock()
	c.Connections[connID] = conn
}

// DeleteConnection removes a connection from in-memory map
func (c *Config) DeleteConnection(connID string) {
	c.ConnectionsMutex.Lock()
	defer c.ConnectionsMutex.Unlock()
	delete(c.Connections, connID)
}

func (c *Config) SetImportRequest(importID string, importSchema *protos.ImportGithubRequest) {
	c.ImportRequestsMutex.Lock()
	defer c.ImportRequestsMutex.Unlock()
	c.ImportRequests[importID] = importSchema
}

func (c *Config) GetImportRequest(importID string) *protos.ImportGithubRequest {
	c.ImportRequestsMutex.RLock()
	defer c.ImportRequestsMutex.RUnlock()

	i, _ := c.ImportRequests[importID]

	return i
}

func (c *Config) DeleteImportRequest(importID string) {
	c.ImportRequestsMutex.Lock()
	defer c.ImportRequestsMutex.Unlock()
	delete(c.ImportRequests, importID)
}

// GetValidation retrieves a schema validation from in-memory map
func (c *Config) GetValidation(validationID string) *common.Validation {
	c.ValidationsMutex.RLock()
	defer c.ValidationsMutex.RUnlock()

	conn, _ := c.Validations[validationID]

	return conn
}

// SetValidation saves a schema validation to in-memory map
func (c *Config) SetValidation(validationID string, conn *common.Validation) {
	c.ValidationsMutex.Lock()
	defer c.ValidationsMutex.Unlock()
	c.Validations[validationID] = conn
}

// DeleteValidation removes a schema validation from in-memory map
func (c *Config) DeleteValidation(validationID string) {
	c.ValidationsMutex.Lock()
	defer c.ValidationsMutex.Unlock()
	delete(c.Validations, validationID)
}
