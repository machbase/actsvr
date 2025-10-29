package main

import (
	"actsvr/siotsvr"
	"os"
)

var version string = "unknown"

func main() {
	siotsvr.Version = version
	os.Exit(siotsvr.Main())
}
