//go:build frontend

package frontend

import "embed"

// EmbedFS is the web UI, in dist, which make builds
//
//go:embed dist
var EmbedFS embed.FS
