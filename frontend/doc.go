// Package frontend is the web UI, which make builds into dist, and which a
// command built with the "frontend" tag embeds, as EmbedFS. It's tracked, so
// go mod tidy, which reads every tag's imports, finds it before the UI is
// built.
package frontend
