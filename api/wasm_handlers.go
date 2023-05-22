package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// getWasmsHandler lists all wasm files stored in plumber server
func (a *API) getWasmsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	files := make([]string, 0)

	for _, wasm := range a.PersistentConfig.WasmFiles {
		files = append(files, wasm.Name)
	}

	WriteJSON(http.StatusOK, files, w)
}
