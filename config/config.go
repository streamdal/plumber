// Package config is used for storing and manipulating (server) state in plumber.
// There should be, at most, a single instance of the plumber config that is
// passed around between various components.
package config

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/imdario/mergo"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/kv"
	stypes "github.com/batchcorp/plumber/server/types"
)

const (
	ConfigFilename = "config.json"
	KVConfigBucket = "plumber"
	KVConfigKey    = "persistent-config"
)

// Config stores Account IDs and the auth_token cookie
type Config struct {
	ClusterID string `json:"-"` // This comes from an environment variable
	PlumberID string `json:"plumber_id"`
	Token     string `json:"token"`
	TeamID    string `json:"team_id"`
	UserID    string `json:"user_id"`

	Connections         map[string]*stypes.Connection `json:"connections"`
	Relays              map[string]*stypes.Relay      `json:"relays"`
	Dynamic             map[string]*stypes.Dynamic    `json:"dynamic_replays"`
	ConnectionsMutex    *sync.RWMutex                 `json:"-"`
	RelaysMutex         *sync.RWMutex                 `json:"-"`
	DynamicReplaysMutex *sync.RWMutex                 `json:"-"`

	enableCluster bool
	kv            kv.IKV
	log           *logrus.Entry
}

// New will attempt to fetch and return an existing config from either NATS or
// the local disk. If neither are available, it will return a new config.
func New(enableCluster bool, k kv.IKV) (*Config, error) {
	var cfg *Config
	var err error

	if enableCluster {
		if k == nil {
			return nil, errors.New("key-value client cannot be nil")
		}

		cfg, err = fetchConfigFromKV(k)
		if err != nil {
			if err == nats.ErrBucketNotFound || err == nats.ErrKeyNotFound {
				return newConfig(enableCluster, k), nil
			}

			return nil, errors.Wrap(err, "unable to fetch config from kv")
		}

		return cfg, nil
	}

	// Not in cluster mode - attempt to read config from disk
	if exists(ConfigFilename) {
		cfg, err = fetchConfigFromFile(ConfigFilename)
		if err != nil {
			logrus.Errorf("unable to load config: %s", err)
		}
	}

	if cfg == nil {
		cfg = newConfig(false, nil)
	}

	return cfg, nil
}

func newConfig(enableCluster bool, k kv.IKV) *Config {
	return &Config{
		Connections:         make(map[string]*stypes.Connection),
		Relays:              make(map[string]*stypes.Relay),
		Dynamic:             make(map[string]*stypes.Dynamic),
		ConnectionsMutex:    &sync.RWMutex{},
		RelaysMutex:         &sync.RWMutex{},
		DynamicReplaysMutex: &sync.RWMutex{},

		kv:            k,
		enableCluster: enableCluster,
		log:           logrus.WithField("pkg", "config"),
	}
}

// Save is a convenience method of persisting the config to KV store or disk
func (c *Config) Save() error {
	data, err := json.Marshal(c)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config to JSON")
	}

	if err := c.writeConfig(data); err != nil {
		c.log.Errorf("unable to save config: %s", err)

		return errors.Wrap(err, "unable to save config")
	}

	return nil
}

func fetchConfigFromKV(k kv.IKV) (*Config, error) {
	var cfg *Config
	var err error

	// Fetch the config from KV
	data, err := k.Get(context.Background(), KVConfigBucket, KVConfigKey)
	if err != nil {
		return nil, err
	}

	// Unmarshal the config
	cfg, err = readConfigBytes(data)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal config from KV")
	}

	cfg.enableCluster = true
	cfg.kv = k
	cfg.log = logrus.WithField("pkg", "config")

	return cfg, nil
}

// readConfig reads a config JSON file into a Config struct
func fetchConfigFromFile(fileName string) (*Config, error) {
	f, err := getConfigJson(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read ~/.batchsh/%s", fileName)
	}

	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read ~/.batchsh/%s", fileName)
	}

	cfg, err := readConfigBytes(data)
	if err != nil {
		return nil, errors.Wrap(err, "could not read config bytes")
	}

	cfg.log = logrus.WithField("pkg", "config")

	return cfg, nil
}

func readConfigBytes(data []byte) (*Config, error) {
	cfg := &Config{
		ConnectionsMutex:    &sync.RWMutex{},
		RelaysMutex:         &sync.RWMutex{},
		DynamicReplaysMutex: &sync.RWMutex{},
		Connections:         make(map[string]*stypes.Connection),
		Relays:              make(map[string]*stypes.Relay),
		Dynamic:             make(map[string]*stypes.Dynamic),
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, errors.Wrapf(err, "could not unmarshal ~/.batchsh/%s", ConfigFilename)
	}

	return cfg, nil
}

// Exists determines if a config file exists yet
func exists(fileName string) bool {
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
func (c *Config) writeConfig(data []byte) error {
	if c.enableCluster {
		if err := c.kv.Put(context.Background(), KVConfigBucket, KVConfigKey, data); err != nil {
			c.log.Errorf("unable to write config to KV: %v", err)

			return errors.Wrap(err, "unable to write config to KV")
		}

		return nil
	}

	// Clustering not enabled - write to disk
	configDir, err := getConfigDir()
	if err != nil {
		c.log.Errorf("unable to determine config dir: %v", err)
		return errors.Wrap(err, "unable to determine config dir")
	}

	// Create dir if needed
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		if err := os.Mkdir(configDir, 0700); err != nil {
			c.log.Errorf("unable to create config directory '%s': %v", configDir, err)

			return errors.Wrapf(err, "unable to create config directory %s", configDir)
		}
	}

	configPath := path.Join(configDir, ConfigFilename)

	f, err := os.OpenFile(configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		c.log.Errorf("failed to open file '%s' for write: %v", configPath, err)
		return err
	}
	defer f.Close()

	_, err = f.Write(data)

	return err
}

func (c *Config) Update(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if err := mergo.Merge(c, cfg); err != nil {
		return errors.Wrap(err, "unable to merge configs")
	}

	if err := c.Save(); err != nil {
		return errors.Wrap(err, "unable to save merged config")
	}

	return nil
}

// getConfigJson attempts to read a user's .batchsh/config.json file; if it
// doesn't exist, it will create an empty json config and return that.
func getConfigJson(fileName string) (*os.File, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return nil, err
	}
	configPath := path.Join(configDir, fileName)

	// Directory ~/.batchsh/ doesn't exist, create it
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := os.Mkdir(configPath, 0700); err != nil {
			return nil, errors.Wrapf(err, "unable to create config directory %s", configPath)
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

// GetConnection retrieves a connection from in-memory map
func (c *Config) GetConnection(connID string) *stypes.Connection {
	c.ConnectionsMutex.RLock()
	defer c.ConnectionsMutex.RUnlock()

	conn, _ := c.Connections[connID]

	return conn
}

// SetConnection saves a connection to in-memory map
func (c *Config) SetConnection(connID string, conn *stypes.Connection) {
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

// GetDynamic returns an in-progress read from the Dynamic map
func (c *Config) GetDynamic(dynamicID string) *stypes.Dynamic {
	c.DynamicReplaysMutex.RLock()
	defer c.DynamicReplaysMutex.RUnlock()

	r, _ := c.Dynamic[dynamicID]

	return r
}

// SetDynamic adds an in-progress read to the Dynamic map
func (c *Config) SetDynamic(dynamicID string, dynamicReplay *stypes.Dynamic) {
	c.DynamicReplaysMutex.Lock()
	defer c.DynamicReplaysMutex.Unlock()

	c.Dynamic[dynamicID] = dynamicReplay
}

// DeleteDynamic removes a dynamic replay from in-memory map
func (c *Config) DeleteDynamic(dynamicID string) {
	c.DynamicReplaysMutex.Lock()
	defer c.DynamicReplaysMutex.Unlock()
	delete(c.Dynamic, dynamicID)
}
