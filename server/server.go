package server

import (
	"actsvr/feature"
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
	"github.com/tochemey/goakt/v3/remote"
)

type Server struct {
	SystemName  string
	Host        string
	Port        int
	actorSystem actor.ActorSystem
}

func (s *Server) Serve(ctx context.Context) error {
	logger := log.New(log.InfoLevel, os.Stdout)

	// create an actor system. Check the reference
	// doc for additional options
	opts := []actor.Option{
		actor.WithRemote(remote.NewConfig(s.Host, s.Port)),
		actor.WithLogger(logger),
	}
	if as, err := actor.NewActorSystem("ACTSVR", opts...); err != nil {
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
	return s.actorSystem.Stop(ctx)
}

func (s *Server) WaitInterrupt() {
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal
}
