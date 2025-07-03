package main

import (
	"actsvr/feature"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/log"
)

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds

	log        log.Logger
	httpServer *http.Server
}

func NewHttpServer() *HttpServer {
	s := &HttpServer{
		Host: "0.0.0.0",
		Port: 8888,
	}
	flag.StringVar(&s.Host, "http-host", s.Host, "the host to bind the HTTP server to")
	flag.IntVar(&s.Port, "http-port", s.Port, "the port to bind the HTTP server to")
	flag.IntVar(&s.KeepAlive, "http-keepalive", 60, "the keep-alive period in seconds for HTTP connections")
	return s
}

func (s *HttpServer) Featured() {
	feature.Add(s)
}

func (s *HttpServer) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	s.log = actorSystem.Logger()
	s.log.Infof("Starting HTTP server on %s:%d", s.Host, s.Port)
	return s.serve()
}

func (s *HttpServer) Stop(ctx context.Context) error {
	s.log.Infof("Stopping HTTP server on %s:%d", s.Host, s.Port)
	return s.httpServer.Shutdown(ctx)
}

func (s *HttpServer) serve() error {
	connContext := func(ctx context.Context, c net.Conn) context.Context {
		if tcpCon, ok := c.(*net.TCPConn); ok && tcpCon != nil {
			tcpCon.SetNoDelay(true)
			if s.KeepAlive > 0 {
				tcpCon.SetKeepAlive(true)
				tcpCon.SetKeepAlivePeriod(time.Duration(s.KeepAlive) * time.Second)
			}
			tcpCon.SetLinger(0)
		}
		return ctx
	}
	s.httpServer = &http.Server{
		ConnContext: connContext,
		Handler:     s.Router(),
	}

	lsnr, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Host, s.Port))
	if err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	go s.httpServer.Serve(lsnr)
	return nil
}
