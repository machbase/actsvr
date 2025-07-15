package siotsvr

import (
	"actsvr/feature"
	"actsvr/server"
	"actsvr/util"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/tochemey/goakt/v3/actor"
)

func Main() int {
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
	return 0
}

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds
	TempDir   string

	dbHost string
	dbPort int
	dbUser string
	dbPass string
	pid    string

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

		pid:    "./siotsvr.pid",
		dbHost: "127.0.0.1",
		dbPort: 5656,
		dbUser: "sys",
		dbPass: "manager",
	}
	flag.StringVar(&s.Host, "http-host", s.Host, "the host to bind the HTTP server to")
	flag.IntVar(&s.Port, "http-port", s.Port, "the port to bind the HTTP server to")
	flag.IntVar(&s.KeepAlive, "http-keepalive", 60, "the keep-alive period in seconds for HTTP connections")
	flag.StringVar(&s.TempDir, "http-tempdir", s.TempDir, "the temporary directory for file uploads")
	flag.StringVar(&s.pid, "pid", s.pid, "the file to store the process ID")

	flag.StringVar(&s.dbHost, "db-host", s.dbHost, "Database host")
	flag.IntVar(&s.dbPort, "db-port", s.dbPort, "Database port")
	flag.StringVar(&s.dbUser, "db-user", s.dbUser, "Database user")
	flag.StringVar(&s.dbPass, "db-pass", s.dbPass, "Database password")

	return s
}

func (s *HttpServer) Featured() {
	feature.Add(s)
}

func (s *HttpServer) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	if err := os.WriteFile(s.pid, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	s.actorSystem = actorSystem
	s.log = actorSystem.Logger().(*util.Log)
	if err := s.openDatabase(); err != nil {
		return err
	}
	s.log.Printf("Starting HTTP server on %s:%d", s.Host, s.Port)
	return s.serve()
}

func (s *HttpServer) Stop(ctx context.Context) error {
	defer func() {
		if err := os.Remove(s.pid); err != nil {
			s.log.Printf("Failed to remove PID file %s: %v", s.pid, err)
		}
	}()
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
			Host:         s.dbHost,
			Port:         s.dbPort,
			TrustUsers:   map[string]string{s.dbUser: s.dbPass},
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
	conn, err := s.machCli.Connect(ctx, api.WithPassword(s.dbUser, s.dbPass))
	if err != nil {
		return nil, err
	}
	return conn, nil
}
