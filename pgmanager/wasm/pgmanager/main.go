package main

import (
	// Packages
	bs "github.com/djthorpe/go-wasmbuild/pkg/bootstrap"
	bsextra "github.com/djthorpe/go-wasmbuild/pkg/bootstrap/extra"
	mvc "github.com/djthorpe/go-wasmbuild/pkg/mvc"
)

func main() {
	// Navigation controller
	controller := bsextra.NavbarController(navbar())

	// Run the application
	mvc.New(controller.Views()[0]).Run()
}

func navbar() mvc.View {
	return bs.NavBar("main",
		bs.WithPosition(bs.Sticky|bs.Top), bs.WithTheme(bs.Dark), bs.WithSize(bs.Medium),
		bs.NavItem("#roles", "Roles"),
	).Label(
		bs.Icon("bootstrap-fill", mvc.WithClass("me-2")), "pgmanager",
	)
}
