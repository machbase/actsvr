package server

import (
	"actsvr/feature"
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
	"github.com/tochemey/goakt/v3/remote"
)

type Server struct {
	SystemName string
	Host       string
	Port       int

	actorSystem actor.ActorSystem
}

func NewServer() *Server {
	svr := &Server{}
	flag.StringVar(&svr.SystemName, "name", "ACTSVR", "the name of the actor system")
	flag.StringVar(&svr.Host, "host", "0.0.0.0", "the host to bind the server to")
	flag.IntVar(&svr.Port, "port", 4000, "the port to bind the server to")
	return svr
}

func (s *Server) Serve(ctx context.Context) error {
	logger := log.New(log.InfoLevel, os.Stdout)

	// create an actor system.
	opts := []actor.Option{
		actor.WithRemote(remote.NewConfig(s.Host, s.Port)),
		actor.WithLogger(logger),
	}
	if as, err := actor.NewActorSystem(s.SystemName, opts...); err != nil {
		logger.Fatal(err)
		os.Exit(1)
	} else {
		s.actorSystem = as
	}

	// start the actor system
	if err := s.actorSystem.Start(ctx); err != nil {
		logger.Fatal(err)
		os.Exit(1)
	}

	// run all the features
	if err := feature.RunFeatures(ctx, s.actorSystem); err != nil {
		logger.Fatal(err)
		os.Exit(1)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.actorSystem == nil || !s.actorSystem.Running() {
		return nil
	}
	return s.actorSystem.Stop(ctx)
}

func (s *Server) WaitInterrupt() {
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal
}
