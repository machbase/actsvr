package main

import (
	"actsvr/server"
	"context"
	"flag"
	"os"
)

func main() {
	svr := &server.Server{}
	flag.StringVar(&svr.SystemName, "name", "ACTSVR", "the name of the actor system")
	flag.StringVar(&svr.Host, "host", "0.0.0.0", "the host to bind the server to")
	flag.IntVar(&svr.Port, "port", 4000, "the port to bind the server to")
	flag.Parse()

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
