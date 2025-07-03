package main

import (
	"actsvr/greetings"

	"actsvr/server"
	"context"
	"flag"
	"os"
)

func main() {
	svr := server.NewServer()
	flag.Parse()

	greetings.Featured()

	ctx := context.Background()
	if err := svr.Serve(ctx); err != nil {
		panic(err)
	}

	svr.WaitInterrupt()

	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	os.Exit(0)
}
