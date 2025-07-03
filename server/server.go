package server

import (
	"actsvr/feature"
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

	logConf        LogConfig
	actorSystem    actor.ActorSystem
	onceParseFlags sync.Once
}

func NewServer() *Server {
	s := &Server{
		logConf: DefaultLogConfig(),
	}
	// actor system configuration
	flag.StringVar(&s.SystemName, "name", "ACTSVR", "the name of the actor system")
	flag.StringVar(&s.Host, "host", "0.0.0.0", "the host to bind the actor system to")
	flag.IntVar(&s.Port, "port", 0, "the port to bind the actor system to")
	// logging configuration
	flag.StringVar(&s.logConf.Filename, "log-filename", s.logConf.Filename, "the log file name")
	flag.IntVar(&s.logConf.MaxSize, "log-max-size", s.logConf.MaxSize, "the maximum size of the log file in megabytes")
	flag.IntVar(&s.logConf.MaxBackups, "log-max-backups", s.logConf.MaxBackups, "the maximum number of log file backups")
	flag.IntVar(&s.logConf.MaxAge, "log-max-age", s.logConf.MaxAge, "the maximum age of the log file in days")
	flag.BoolVar(&s.logConf.Compress, "log-compress", s.logConf.Compress, "whether to compress the log file")
	flag.BoolVar(&s.logConf.LocalTime, "log-localtime", s.logConf.LocalTime, "whether to use local time in the log file")
	flag.BoolVar(&s.logConf.Append, "log-append", s.logConf.Append, "whether to append to the log file or overwrite it")
	flag.StringVar(&s.logConf.Timeformat, "log-timeformat", s.logConf.Timeformat, "the time format to use in the log file")
	flag.BoolVar(&s.logConf.Debug, "log-debug", s.logConf.Debug, "whether to enable debug logging")

	return s
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
	opts := []actor.Option{
		actor.WithLogger(NewLog(s.logConf)),
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
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal
}
