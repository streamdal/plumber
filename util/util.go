package util

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	sdk "github.com/streamdal/streamdal/sdks/go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func DurationSec(durationSec interface{}) time.Duration {
	if v, ok := durationSec.(int32); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(uint32); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(int64); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(int); ok {
		return time.Duration(v) * time.Second
	}

	return 0
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
var charRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

func RandomString(n int) string {
	b := make([]rune, n)

	for i := range b {
		b[i] = charRunes[rand.Intn(len(charRunes))]
	}

	return string(b)
}

// Gunzip decompresses a slice of bytes and returns a slice of decompressed
// bytes or an error.
func Gunzip(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)

	var r io.Reader

	r, err := gzip.NewReader(b)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new reader")
	}

	var resB bytes.Buffer

	if _, err := resB.ReadFrom(r); err != nil {
		return nil, errors.Wrap(err, "unable to read data from reader")
	}

	return resB.Bytes(), nil
}

func DirsExist(dirs []string) error {
	var errs []string

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("'%s' does not exist", dir))
		}
	}

	if errs == nil {
		return nil
	}

	return errors.New(strings.Join(errs, "; "))
}

// WriteError is a wrapper for logging an error + writing to an error channel.
// Both the logger and error channel can be nil.
func WriteError(l *logrus.Entry, errorCh chan<- *records.ErrorRecord, err error) {
	if l != nil {
		l.Error(err)
	}

	if errorCh != nil {
		errorCh <- &records.ErrorRecord{
			OccurredAtUnixTsUtc: time.Now().UTC().UnixNano(),
			Error:               err.Error(),
		}
	}
}

// SetupStreamdalSDK creates a new Streamdal client if the integration is enabled.
// If integration opts are nil or if integration is not enabled, return nil.
func SetupStreamdalSDK(relayOpts *opts.RelayOptions, l *logrus.Entry) (*sdk.Streamdal, error) {
	// Either no StreamIntegrationOptions or integration is disabled
	if relayOpts == nil || relayOpts.StreamdalIntegrationOptions == nil || !relayOpts.StreamdalIntegrationOptions.StreamdalIntegrationEnable {
		return nil, nil
	}

	// Integration is enabled; create client
	sc, err := sdk.New(&sdk.Config{
		ServerURL:   relayOpts.StreamdalIntegrationOptions.StreamdalIntegrationServerAddress,
		ServerToken: relayOpts.StreamdalIntegrationOptions.StreamdalIntegrationAuthToken,
		ServiceName: relayOpts.StreamdalIntegrationOptions.StreamdalIntegrationServiceName,
		Logger:      l,
		ClientType:  sdk.ClientTypeSDK,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to create new streamdal client")
	}

	return sc, nil
}

func MapInterfaceToString(input map[string]interface{}) map[string]string {
	out := make(map[string]string)

	for k, v := range input {
		out[k] = fmt.Sprintf("%v", v)
	}

	return out
}

func DerefTime(t *time.Time) int64 {
	if t == nil {
		return 0
	}

	return t.UTC().Unix()
}

func DerefUint32(v *uint32) uint32 {
	if v == nil {
		return 0
	}

	return *v
}

func DerefString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func DerefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}

	return *v
}

func DerefInt16(v *int16) int16 {
	if v == nil {
		return 0
	}

	return *v
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func GenerateTLSConfig(caCert, clientCert, clientKey string, skipVerify bool, mTLS tls.ClientAuthType) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	if len(caCert) > 0 {
		if FileExists(caCert) {
			// CLI input, read from file
			pemCerts, err := ioutil.ReadFile(caCert)
			if err == nil {
				certpool.AppendCertsFromPEM(pemCerts)
			}
		} else {
			// Server input, contents of the certificate
			certpool.AppendCertsFromPEM([]byte(caCert))
		}
	}

	// Import client certificate/key pair
	var cert tls.Certificate
	var err error
	if len(clientCert) > 0 && len(clientKey) > 0 {
		if FileExists(clientCert) {
			// CLI input, read from file
			cert, err = tls.LoadX509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, errors.Wrap(err, "unable to load client certificate")
			}
		} else {
			// Server input, contents of the certificate
			cert, err = tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
			if err != nil {
				return nil, errors.Wrap(err, "unable to load client certificate")
			}
		}

		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse certificate")
		}
	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         mTLS,
		ClientCAs:          nil,
		InsecureSkipVerify: skipVerify,
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
	}, nil
}

// GenerateNATSAuthJWT accepts either a path to a .creds file containing JWT credentials or the credentials
// directly in string format, and returns the correct nats connection authentication options
func GenerateNATSAuthJWT(creds string) ([]nats.Option, error) {
	opts := make([]nats.Option, 0)

	// Creds as a file
	if FileExists(creds) {
		return append(opts, nats.UserCredentials(creds)), nil
	}

	// Creds as string
	userCB, err := nkeys.ParseDecoratedJWT([]byte(creds))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse user credentials")
	}

	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.ParseDecoratedNKey([]byte(creds))
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse nkey from credentials")
		}
		defer kp.Wipe()

		sig, err := kp.Sign(nonce)
		if err != nil {
			return nil, err
		}
		return sig, nil
	}

	jwtFunc := func(o *nats.Options) error {
		o.UserJWT = func() (string, error) {
			return userCB, nil
		}
		o.SignatureCB = sigCB
		return nil
	}
	return append(opts, jwtFunc), nil
}

// GenerateNATSAuthNKey accepts either a path to a file containing a Nkey seed, or the Nkey seed
// directly in string format, and returns the correct nats connection authentication options
func GenerateNATSAuthNKey(nkeyPath string) ([]nats.Option, error) {
	opts := make([]nats.Option, 0)

	// Creds as a file
	if FileExists(nkeyPath) {
		// CLI, pass via filepath
		nkeyCreds, err := nats.NkeyOptionFromSeed(nkeyPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to load nkey")
		}

		return append(opts, nkeyCreds), nil
	}

	// Server conn, pass via string
	kp, err := nkeys.ParseDecoratedNKey([]byte(nkeyPath))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse nkey data")
	}

	defer kp.Wipe()

	pubKey, err := kp.PublicKey()
	if err != nil {
		return nil, errors.Wrap(err, "unable to find public key in nkey data")
	}
	if !nkeys.IsValidPublicUserKey(pubKey) {
		return nil, fmt.Errorf("not a valid nkey user seed")
	}

	sigCB := func(nonce []byte) ([]byte, error) {
		sig, _ := kp.Sign(nonce)

		return sig, nil
	}

	return append(opts, nats.Nkey(pubKey, sigCB)), nil

}

// IsBase64 determines if a string is base64 encoded or not
// We store bus headers in map[string]string and binary headers get encoded as base64
// In order to replay headers properly, we need
func IsBase64(v string) bool {
	decoded, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return false
	}

	// Definitely not base64
	if base64.StdEncoding.EncodeToString(decoded) != v {
		return false
	}

	// Might be base64, some numbers will pass DecodeString() 🤦: https://go.dev/play/p/vlDi7CLw2qu
	num, err := strconv.Atoi(v)
	if err == nil && fmt.Sprintf("%d", num) == v {
		// Input is a number, return false
		return false
	}

	return true
}
