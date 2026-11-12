// Package frontend is the web UI, which a server built with the "frontend"
// tag embeds. build.js copies it to build/frontend, with the UI it builds,
// in dist.
package frontend

import "embed"

// EmbedFS is the UI, in dist
//
//go:embed dist
var EmbedFS embed.FS
