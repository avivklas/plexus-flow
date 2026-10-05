package api

import (
	_ "embed"
)

// UIHTML is the embedded single-page visualization application for Plexus-Flow.
//
//go:embed web/index.html
var UIHTML string
