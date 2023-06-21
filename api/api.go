package api

import (
	"bytes"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/bus"
	"github.com/batchcorp/plumber/config"
)

//go:embed all:assets
var staticFiles embed.FS

var (
	ErrMissingConfig           = errors.New("config cannot be nil")
	ErrMissingPersistentConfig = errors.New("PersistentConfig cannot be nil")
	ErrMissingBus              = errors.New("Bus cannot be nil")
	ErrEmptyListenAddress      = errors.New("ListenAddress cannot be empty")
)

type API struct {
	*Config
	log *logrus.Entry
}

type ResponseJSON struct {
	Status  int               `json:"status"`
	Message string            `json:"message"`
	Values  map[string]string `json:"values,omitempty"`
	Errors  string            `json:"errors,omitempty"`
}

type Config struct {
	PersistentConfig *config.Config
	Bus              bus.IBus
	ListenAddress    string
	Version          string
}

func Start(cfg *Config) (*http.Server, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	consoleMap := map[string]string{
		"/console/*filepath": "assets",
		"/_astro/*filepath":  "assets/_astro",
		"/images/*filepath":  "assets/images",
		"/ruleset/*filepath": "assets/ruleset",
		"/slack/*filepath":   "assets/slack",
	}

	a := &API{
		Config: cfg,
		log:    logrus.WithField("pkg", "api"),
	}

	a.log.Debugf("starting API server on %s", cfg.ListenAddress)

	router := httprouter.New()

	// Redirect / to the console
	router.HandlerFunc("GET", "/", http.RedirectHandler("/console", http.StatusTemporaryRedirect).ServeHTTP)

	// Setup embedded console paths
	for pathURL, pathFile := range consoleMap {
		content, err := fs.Sub(fs.FS(staticFiles), pathFile)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to mount '%s' at '%s'", pathFile, pathURL)
		}
		router.ServeFiles(pathURL, http.FS(content))
	}

	router.HandlerFunc("GET", "/health-check", a.healthCheckHandler)
	router.HandlerFunc("GET", "/version", a.versionHandler)

	router.Handle("GET", "/v1/ruleset/:ruleset_id/rules", a.getRulesHandler)
	router.Handle("POST", "/v1/ruleset/:ruleset_id/rules", a.createRuleHandler)
	router.Handle("PUT", "/v1/ruleset/:ruleset_id/rules/:id", a.updateRuleHandler)
	router.Handle("DELETE", "/v1/ruleset/:ruleset_id/rules/:id", a.deleteRuleHandler)

	router.Handle("GET", "/v1/ruleset", a.getRuleSetsHandler)
	router.Handle("POST", "/v1/ruleset", a.createRuleSetHandler)
	router.Handle("PUT", "/v1/ruleset/:ruleset_id", a.updateRuleSetHandler)
	router.Handle("DELETE", "/v1/ruleset/:ruleset_id", a.deleteRuleSetHandler)
	router.Handle("GET", "/v1/ruleset/:ruleset_id", a.getRuleSetHandler)

	router.Handle("GET", "/v1/slack", a.getSlackConfigHandler)
	router.Handle("POST", "/v1/slack", a.updateSlackConfigHandler)

	// TODO: remove
	router.Handle("GET", "/v1/temp-populate", a.tempPopulateHandler)

	router.Handle("GET", "/v1/wasm", a.getWasmsHandler)

	router.Handler("GET", "/metrics", promhttp.Handler())

	corsOrigins, ok := os.LookupEnv("CORS_ORIGINS")
	if !ok {
		corsOrigins = "http://localhost:3000,http://localhost:9191"
	}

	corsRouter := cors.New(cors.Options{
		AllowedOrigins: strings.Split(corsOrigins, ","),

		AllowedMethods: []string{
			http.MethodOptions,
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		},
		AllowedHeaders:   []string{"*"},
		ExposedHeaders:   []string{"redirect_uri", "set-cookie"},
		AllowCredentials: true,
	}).Handler(router)

	srv := &http.Server{
		Addr:    cfg.ListenAddress,
		Handler: corsRouter,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				a.log.Errorf("unable to srv.ListenAndServe: %s", err)
			}
		}
	}()

	return srv, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return ErrMissingConfig
	}

	if cfg.PersistentConfig == nil {
		return ErrMissingPersistentConfig
	}

	if cfg.Bus == nil {
		return ErrMissingBus
	}

	if cfg.ListenAddress == "" {
		return ErrEmptyListenAddress
	}

	return nil
}

func (a *API) healthCheckHandler(rw http.ResponseWriter, r *http.Request) {
	WriteJSON(http.StatusOK, map[string]string{"status": "ok"}, rw)
}

func (a *API) versionHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")

	response := &ResponseJSON{Status: http.StatusOK, Message: "batchcorp/plumber " + a.Version}

	WriteJSON(http.StatusOK, response, rw)
}

func WriteJSON(statusCode int, data interface{}, w http.ResponseWriter) {
	w.Header().Add("Content-type", "application/json")

	jsonData, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(500)
		logrus.Errorf("Unable to marshal data in WriteJSON: %s", err)
		return
	}

	w.WriteHeader(statusCode)

	if _, err := w.Write(jsonData); err != nil {
		logrus.Errorf("Unable to write response data: %s", err)
		return
	}
}

func WriteErrorJSON(statusCode int, msg string, w http.ResponseWriter) {
	WriteJSON(statusCode, map[string]string{"error": msg}, w)
}

func WriteSuccessJSON(statusCode int, msg string, w http.ResponseWriter) {
	WriteJSON(statusCode, map[string]string{"msg": msg}, w)
}

func DecodeBody(input io.ReadCloser, into interface{}) error {
	body, err := io.ReadAll(input)
	if err != nil || len(body) == 0 {
		if err == nil {
			err = errors.New("body is empty")
		}

		return errors.Wrap(err, "failed to parse input body")
	}
	defer input.Close()

	if err := json.Unmarshal(body, into); err != nil {
		return errors.Wrap(err, "failed to unmarshal body")
	}

	return nil
}

func DecodeProtoBody(input io.ReadCloser, into proto.Message) error {
	body, err := io.ReadAll(input)
	if err != nil || len(body) == 0 {
		if err == nil {
			err = errors.New("body is empty")
		}

		return errors.Wrap(err, "failed to parse input body")
	}
	defer input.Close()

	if err := jsonpb.Unmarshal(bytes.NewBuffer(body), into); err != nil {
		return errors.Wrap(err, "failed to unmarshal body")
	}

	return nil
}
