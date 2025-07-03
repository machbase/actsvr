package main

import (
	"actsvr/server"
	"context"
	"os"
)

func main() {
	httpSvr := NewHttpServer()
	httpSvr.Featured()

	ctx := context.Background()
	svr := server.NewServer()
	if err := svr.Serve(ctx); err != nil {
		panic(err)
	}
	svr.WaitInterrupt()
	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	os.Exit(0)
}
