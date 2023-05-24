// Package config is used for storing and manipulating the plumber config.
// There should be, at most, a single instance of the plumber config that is
// passed around between various components.
//
// If running in cluster mode, config will write the config to NATS. If running
// locally, the config will be saved to ~/.batchsh/plumber.json
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/Masterminds/semver"
	"github.com/imdario/mergo"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/kv"
	"github.com/batchcorp/plumber/options"
	stypes "github.com/batchcorp/plumber/server/types"
)

const (
	ConfigDir      = ".batchsh"
	ConfigFilename = "plumber.json"
	KVConfigBucket = "plumber"
	KVConfigKey    = "persistent-config"
)

// Config stores Account IDs and the auth_token cookie
type Config struct {
	ClusterID       string `json:"-"` // This comes from an environment variable
	PlumberID       string `json:"plumber_id"`
	Token           string `json:"token"`
	TeamID          string `json:"team_id"`
	UserID          string `json:"user_id"`
	EnableTelemetry bool   `json:"enable_telemetry"`
	LastVersion     string `json:"last_version"`
	SlackToken      string `json:"slack_token"`

	Connections      map[string]*stypes.Connection `json:"connections"`
	Relays           map[string]*stypes.Relay      `json:"relays"`
	Tunnels          map[string]*stypes.Tunnel     `json:"tunnels"`
	WasmFiles        map[string]*stypes.WasmFile   `json:"wasm_files"`
	RuleSets         map[string]*stypes.RuleSet    `json:"rulesets"`
	ConnectionsMutex *sync.RWMutex                 `json:"-"`
	RelaysMutex      *sync.RWMutex                 `json:"-"`
	TunnelsMutex     *sync.RWMutex                 `json:"-"`
	WasmFilesMutex   *sync.RWMutex                 `json:"-"`
	RuleSetMutex     *sync.RWMutex                 `json:"-"`

	enableCluster bool          `json:"-"`
	KV            kv.IKV        `json:"-"`
	log           *logrus.Entry `json:"-"`
}

// New will attempt to fetch and return an existing config from either NATS or
// the local disk. If neither are available, it will return a new config.
func New(enableCluster bool, k kv.IKV) (*Config, error) {
	var cfg *Config
	var err error

	defer func() {
		// When config is nil and an error is returned from New(),
		// allow this defer to exit without panicking
		if cfg == nil {
			return
		}

		cfg.LastVersion = options.VERSION

		// Old versions of plumber incorrectly defaulted plumber id to plumber1;
		// everyone should have a unique plumber id
		if cfg.PlumberID == "plumber1" {
			cfg.PlumberID = getPlumberID()
		}
	}()

	if enableCluster {
		if k == nil {
			return nil, errors.New("key value store not initialized - are you running in server mode?")
		}

		cfg, err = fetchConfigFromKV(k)
		if err != nil {
			if err == nats.ErrBucketNotFound || err == nats.ErrKeyNotFound {
				return newConfig(enableCluster, k), nil
			}

			return nil, errors.Wrap(err, "unable to fetch config from KV")
		}
	}

	// Not in cluster mode - attempt to read config from disk
	if cfg == nil && exists(ConfigFilename) {
		cfg, err = fetchConfigFromFile(ConfigFilename)
		if err != nil {
			logrus.Errorf("unable to load config: %s", err)
		}
	}

	// Cleanup old config file (if exists)
	if exists("config.json") {
		if err := remove("config.json"); err != nil {
			logrus.Warningf("unable to remove old config file: %s", err)
		}
	}

	var initialRun bool

	if cfg == nil {
		initialRun = true

		cfg = newConfig(false, nil)
	}

	// Should we perform an interactive config?
	if requireReconfig(initialRun, cfg) {
		cfg.Configure()
	}

	return cfg, nil
}

func requireReconfig(initialRun bool, cfg *Config) bool {
	// Don't configure if NOT in terminal
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.Debugf("detected non-terminal output")
		return false
	}

	// Should not ever reach this case but avoid a panic just incase
	if cfg == nil {
		logrus.Warningf("bug? cfg is nil in requireReconfig")
		return false
	}

	// No reason to ask to reconfigure if we can't figure out old version
	currentVersion, err := semver.NewVersion(options.VERSION)
	if err != nil {
		logrus.Warningf("unable to parse current version: %s", err)
		return false
	}

	// Brand new config or config doesn't contain LastVersion yet
	if initialRun || cfg.LastVersion == "" {
		return true
	}

	// We are probably dealing with an old plumber if we can't figure out the
	// version - reconfigure
	lastVersion, err := semver.NewVersion(cfg.LastVersion)
	if err != nil {
		logrus.Warningf("unable to parse last version: %s", err)
		return true
	}

	if currentVersion.Minor() != lastVersion.Minor() {
		return true
	}

	// Shouldn't ever hit this
	return false
}

func (c *Config) Configure() {
	// No need to ask about telemetry if it's already enabled
	if !c.EnableTelemetry {
		c.askTelemetry()
	}
}

func (c *Config) askTelemetry() {
	telemetryDescription := `If telemetry is enabled, plumber will collect the following anonymous telemetry data:

> General
	- PlumberID (a unique, randomly generated ID for this plumber instance)
	- Plumber version
	- OS and architecture
	- Plumber mode (server or CLI)

> For CLI
	- Plumber action (read, write, relay, etc.)
	- Backend used (kafka, rabbitmq, nats, etc.)
	- Data format used for read or write (json, protobuf, etc.)
	- If reading, whether continuous mode is used
	- If using protobuf, whether file descriptors are used

> For server
	- Number of connections, relays, tunnels
	- Server uptime
	- ClusterID
	- gRPC methods used (create relay, stop tunnel, etc.)

NOTE: We do NOT collect ANY personally identifiable or confidential information.

You can read this statement here: https://docs.streamdal.com/plumber/telemetry
`
	fmt.Printf(telemetryDescription + "\n")

	enableTelemetry, err := askYesNo("Do you want to enable telemetry?", "N")
	if err != nil {
		c.log.Fatalf("unable to configure plumber: %s", err)
	}

	if enableTelemetry {
		fmt.Printf("\nNICE! Thank you for opting in! This will help us improve plumber :)\n\n")
	}

	c.EnableTelemetry = enableTelemetry
}

func askYesNo(question, defaultAnswer string) (bool, error) {
	if defaultAnswer != "" {
		fmt.Printf(question+" [y/n (default: %s)]: ", defaultAnswer)
	} else {
		fmt.Print(question + " [y/n]: ")
	}

	var answer string

	i, err := fmt.Scanln(&answer)

	// Scan() doesn't return on only newline and empty string
	// Scanln() will error on only new line and empty string
	if err != nil && !strings.Contains(err.Error(), "unexpected newline") {
		return false, fmt.Errorf("unable to read input: %s", err)
	}

	if i == 0 {
		if defaultAnswer != "" {
			answer = defaultAnswer
		}
	}

	answer = strings.ToLower(answer)

	if answer == "y" || answer == "yes" {
		return true, nil
	}

	if answer == "n" || answer == "no" {
		return false, nil
	}

	fmt.Println("invalid input")
	return askYesNo(question, defaultAnswer)
}

func newConfig(enableCluster bool, k kv.IKV) *Config {
	return &Config{
		PlumberID:        getPlumberID(),
		Connections:      make(map[string]*stypes.Connection),
		Relays:           make(map[string]*stypes.Relay),
		Tunnels:          make(map[string]*stypes.Tunnel),
		WasmFiles:        make(map[string]*stypes.WasmFile),
		RuleSets:         make(map[string]*stypes.RuleSet),
		ConnectionsMutex: &sync.RWMutex{},
		RelaysMutex:      &sync.RWMutex{},
		TunnelsMutex:     &sync.RWMutex{},
		WasmFilesMutex:   &sync.RWMutex{},
		RuleSetMutex:     &sync.RWMutex{},

		KV:            k,
		enableCluster: enableCluster,
		log:           logrus.WithField("pkg", "config"),
	}
}

func getPlumberID() string {
	return uuid.NewV4().String()
}

// Save is a convenience method of persisting the config to KV store or disk
func (c *Config) Save() error {
	data, err := json.MarshalIndent(c, "", "\t")
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
	cfg.KV = k
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
	// Hack: handle flag name change in marshaled data when upgrading plumber cluster
	// Some flags were changed in v2, such as BatchshGRPCCollectorAddress, which is now
	// StreamdalGRPCCollectorAddress
	tmp := string(data)
	if strings.Contains(tmp, "\"Batchsh") {
		tmp = strings.Replace(tmp, "\"Batchsh", "\"Streamdal", -1)
		println(tmp)
		data = []byte(tmp)
	}

	cfg := &Config{
		ConnectionsMutex: &sync.RWMutex{},
		RelaysMutex:      &sync.RWMutex{},
		TunnelsMutex:     &sync.RWMutex{},
		WasmFilesMutex:   &sync.RWMutex{},
		RuleSetMutex:     &sync.RWMutex{},
		Connections:      make(map[string]*stypes.Connection),
		Relays:           make(map[string]*stypes.Relay),
		Tunnels:          make(map[string]*stypes.Tunnel),
		WasmFiles:        make(map[string]*stypes.WasmFile),
		RuleSets:         make(map[string]*stypes.RuleSet),
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

func remove(fileName string) error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}
	configPath := path.Join(configDir, fileName)

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil
	}

	return os.Remove(configPath)
}

// WriteConfig writes a Batch struct as JSON into a config.json file
func (c *Config) writeConfig(data []byte) error {
	if c.enableCluster {
		if err := c.KV.Put(context.Background(), KVConfigBucket, KVConfigKey, data); err != nil {
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

	return path.Join(homeDir, ConfigDir), nil
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
	if c.Relays == nil {
		c.Relays = make(map[string]*stypes.Relay)
	}
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

// GetTunnel returns an entry from the Tunnels map
func (c *Config) GetTunnel(tunnelID string) *stypes.Tunnel {
	c.TunnelsMutex.RLock()
	defer c.TunnelsMutex.RUnlock()

	r, _ := c.Tunnels[tunnelID]

	return r
}

// SetTunnel adds an entry to the Tunnels map
func (c *Config) SetTunnel(tunnelID string, tunnel *stypes.Tunnel) {
	c.TunnelsMutex.Lock()
	defer c.TunnelsMutex.Unlock()

	if c.Tunnels == nil {
		c.Tunnels = make(map[string]*stypes.Tunnel)
	}
	c.Tunnels[tunnelID] = tunnel
}

// DeleteTunnel removes a tunnel from in-memory map
func (c *Config) DeleteTunnel(tunnelID string) {
	c.TunnelsMutex.Lock()
	defer c.TunnelsMutex.Unlock()
	delete(c.Tunnels, tunnelID)
}

// GetWasmFile returns an entry from the WasmFiles map
func (c *Config) GetWasmFile(fileName string) *stypes.WasmFile {
	c.WasmFilesMutex.RLock()
	defer c.WasmFilesMutex.RUnlock()

	r, _ := c.WasmFiles[fileName]

	return r
}

// SetWasmFile adds an entry to the WasmFiles map
func (c *Config) SetWasmFile(fileName string, tunnel *stypes.WasmFile) {
	c.WasmFilesMutex.Lock()
	defer c.WasmFilesMutex.Unlock()

	if c.WasmFiles == nil {
		c.WasmFiles = make(map[string]*stypes.WasmFile)
	}
	c.WasmFiles[fileName] = tunnel
}

// DeleteWasmFile removes a wasm file from in-memory map
func (c *Config) DeleteWasmFile(fileName string) {
	c.WasmFilesMutex.Lock()
	defer c.WasmFilesMutex.Unlock()
	delete(c.WasmFiles, fileName)
}

// GetRuleSet returns an entry from the RuleSets map
func (c *Config) GetRuleSet(id string) *stypes.RuleSet {
	c.RuleSetMutex.RLock()
	defer c.RuleSetMutex.RUnlock()

	r, _ := c.RuleSets[id]

	return r
}

// SetRuleSet adds an entry to the RuleSets map
func (c *Config) SetRuleSet(id string, rs *stypes.RuleSet) {
	c.RuleSetMutex.Lock()
	defer c.RuleSetMutex.Unlock()

	if c.RuleSets == nil {
		c.RuleSets = make(map[string]*stypes.RuleSet)
	}
	c.RuleSets[id] = rs
}

// DeleteRuleSet removes a rule set from in-memory map
func (c *Config) DeleteRuleSet(rulesetID string) {
	c.RuleSetMutex.Lock()
	defer c.TunnelsMutex.Unlock()
	delete(c.RuleSets, rulesetID)
}
