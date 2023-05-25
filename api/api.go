package api

import (
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"

	"github.com/batchcorp/plumber/config"
	"github.com/pkg/errors"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

//go:embed all:assets
var staticFiles embed.FS

type API struct {
	Version          string
	ListenAddress    string
	PersistentConfig *config.Config
	log              *logrus.Entry
}

type ResponseJSON struct {
	Status  int               `json:"status"`
	Message string            `json:"message"`
	Values  map[string]string `json:"values,omitempty"`
	Errors  string            `json:"errors,omitempty"`
}

func Start(cfg *config.Config, listenAddress, version string) (*http.Server, error) {
	// Define console static file server
	htmlContent, err := fs.Sub(fs.FS(staticFiles), "assets")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create static file server")
	}

	astroContent, err := fs.Sub(fs.FS(staticFiles), "assets/_astro")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create static file server astro assets")
	}

	imagesContent, err := fs.Sub(fs.FS(staticFiles), "assets/images")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create static file server for images")
	}

	a := &API{
		Version:          version,
		ListenAddress:    listenAddress,
		PersistentConfig: cfg,
		log:              logrus.WithField("pkg", "api"),
	}

	a.log.Debugf("starting API server on %s", listenAddress)

	router := httprouter.New()

	// Redirect / to the console
	router.HandlerFunc("GET", "/", http.RedirectHandler("/console", http.StatusTemporaryRedirect).ServeHTTP)
	router.ServeFiles("/console/*filepath", http.FS(htmlContent))
	router.ServeFiles("/_astro/*filepath", http.FS(astroContent))
	router.ServeFiles("/images/*filepath", http.FS(imagesContent))

	router.HandlerFunc("GET", "/health-check", a.healthCheckHandler)
	router.HandlerFunc("GET", "/version", a.versionHandler)

	router.Handle("GET", "/v1/ruleset", a.getRuleSetsHandler)
	router.Handle("POST", "/v1/ruleset", a.createRuleSetHandler)
	router.Handle("DELETE", "/v1/ruleset/:id", a.deleteRuleSetHandler)
	router.Handle("GET", "/v1/ruleset/:id", a.getRuleSetsHandler)

	router.Handle("GET", "/v1/wasm", a.getWasmsHandler)

	router.Handler("GET", "/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    listenAddress,
		Handler: router,
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
