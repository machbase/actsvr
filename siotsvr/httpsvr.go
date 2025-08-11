package siotsvr

import (
	"actsvr/feature"
	"actsvr/util"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/tochemey/goakt/v3/actor"
)

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds
	TempDir   string

	actorSystem actor.ActorSystem
	machCli     *machcli.Database
	log         *util.Log
	httpServer  *http.Server
	router      *gin.Engine
}

func NewHttpServer() *HttpServer {
	s := &HttpServer{
		Host:    "0.0.0.0",
		Port:    8888,
		TempDir: "/tmp",
	}
	flag.StringVar(&s.Host, "http-host", s.Host, "the host to bind the HTTP server to")
	flag.IntVar(&s.Port, "http-port", s.Port, "the port to bind the HTTP server to")
	flag.IntVar(&s.KeepAlive, "http-keepalive", 60, "the keep-alive period in seconds for HTTP connections")
	flag.StringVar(&s.TempDir, "http-tempdir", s.TempDir, "the temporary directory for file uploads")

	return s
}

func (s *HttpServer) Featured() {
	feature.Add(s)
}

func (s *HttpServer) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	s.actorSystem = actorSystem
	s.log = actorSystem.Logger().(*util.Log)
	if err := s.openDatabase(); err != nil {
		return err
	}
	s.log.Printf("Starting HTTP server on %s:%d", s.Host, s.Port)
	return s.serve()
}

func (s *HttpServer) Stop(ctx context.Context) error {
	s.closeDatabase()
	s.log.Printf("Stopping HTTP server on %s:%d", s.Host, s.Port)
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

func (s *HttpServer) openDatabase() error {
	if s.machCli == nil {
		db, err := machcli.NewDatabase(&machcli.Config{
			Host:         machConfig.dbHost,
			Port:         machConfig.dbPort,
			TrustUsers:   map[string]string{machConfig.dbUser: machConfig.dbPass},
			MaxOpenConn:  -1,
			MaxOpenQuery: -1,
		})
		if err != nil {
			return err
		}
		s.machCli = db
	}
	return nil
}

func (s *HttpServer) closeDatabase() {
	if s.machCli != nil {
		s.machCli.Close()
		s.machCli = nil
	}
}

func (s *HttpServer) openConn(ctx context.Context) (api.Conn, error) {
	if err := s.openDatabase(); err != nil {
		return nil, err
	}
	conn, err := s.machCli.Connect(ctx, api.WithPassword(machConfig.dbUser, machConfig.dbPass))
	if err != nil {
		return nil, err
	}
	return conn, nil
}
