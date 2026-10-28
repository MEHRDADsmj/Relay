package web

import (
	"net/http"
	"strings"
)

// Mount returns a handler that serves the UI under /ui and the API elsewhere.
func Mount(api, ui http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ui") {
			ui.ServeHTTP(w, r)
			return
		}
		api.ServeHTTP(w, r)
	})
}
