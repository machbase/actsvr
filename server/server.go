package server

import (
	"actsvr/feature"
	"actsvr/util"
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/remote"
)

type Server struct {
	SystemName string
	Host       string
	Port       int

	log             *util.Log
	actorSystem     actor.ActorSystem
	onceParseFlags  sync.Once
	interruptSignal chan os.Signal
}

func NewServer(opts ...Option) *Server {
	s := &Server{}
	// apply options
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type Option func(*Server)

func WithName(name string) Option {
	return func(s *Server) {
		s.SystemName = name
	}
}

func WithHost(host string) Option {
	return func(s *Server) {
		s.Host = host
	}
}

func WithPort(port int) Option {
	return func(s *Server) {
		s.Port = port
	}
}

func WithLog(log *util.Log) Option {
	return func(s *Server) {
		s.log = log
	}
}

func (s *Server) parseFlags() {
	s.onceParseFlags.Do(func() {
		flag.Parse()
	})
}

func (s *Server) Serve(ctx context.Context) error {
	// parse the flags only once
	s.parseFlags()

	// create an actor system.
	opts := []actor.Option{}
	if s.log != nil {
		opts = append(opts, actor.WithLogger(s.log))
	}
	if s.Port > 0 {
		opts = append(opts, actor.WithRemote(remote.NewConfig(s.Host, s.Port)))
	}
	if as, err := actor.NewActorSystem(s.SystemName, opts...); err != nil {
		return err
	} else {
		s.actorSystem = as
	}

	// start the actor system
	if err := s.actorSystem.Start(ctx); err != nil {
		return err
	}

	// run all the features
	if err := feature.StartFeatures(ctx, s.actorSystem); err != nil {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.actorSystem == nil || !s.actorSystem.Running() {
		return nil
	}
	if err := feature.StopFeatures(ctx); err != nil {
		return err
	}
	return s.actorSystem.Stop(ctx)
}

func (s *Server) WaitInterrupt() {
	s.interruptSignal = make(chan os.Signal, 1)
	signal.Notify(s.interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-s.interruptSignal
}

func (s *Server) Notify() {
	if s.interruptSignal == nil {
		return
	}
	signal.Stop(s.interruptSignal)
	close(s.interruptSignal)
	s.interruptSignal = nil
}

func (s *Server) ActorSystem() actor.ActorSystem {
	return s.actorSystem
}
