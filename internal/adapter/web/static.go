package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

func staticHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("web: static embed: " + err.Error())
	}
	return http.StripPrefix("/ui/static/", http.FileServer(http.FS(sub)))
}
