// Package web embeds the UI so the server ships as a single binary.
package web

import "embed"

//go:embed index.html
var Files embed.FS
