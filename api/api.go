package api

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

type API struct {
	Version       string
	ListenAddress string
	log           *logrus.Entry
}

type ResponseJSON struct {
	Status  int               `json:"status"`
	Message string            `json:"message"`
	Values  map[string]string `json:"values,omitempty"`
	Errors  string            `json:"errors,omitempty"`
}

func Start(listenAddress, version string) error {
	a := &API{
		Version:       version,
		ListenAddress: listenAddress,
		log:           logrus.WithField("pkg", "api"),
	}

	router := httprouter.New()

	router.HandlerFunc("GET", "/health-check", a.healthCheckHandler)
	router.HandlerFunc("GET", "/version", a.versionHandler)
	router.Handler("GET", "/metrics", promhttp.Handler())

	a.log.Infof("starting API server on %s", listenAddress)

	return http.ListenAndServe(listenAddress, router)
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
