package trjd

import (
	"actsvr/feature"
	"actsvr/server"
	"actsvr/util"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/tochemey/goakt/v3/actor"
)

func Main() int {
	httpSvr := NewHttpServer()
	httpSvr.Featured()

	master := NewMaster(Config{})
	master.Featured()

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

	dbHost  string
	dbPort  int
	dbUser  string
	dbPass  string
	dbTable string

	actorSystem actor.ActorSystem
	log         *util.Log
	httpServer  *http.Server
}

func NewHttpServer() *HttpServer {
	s := &HttpServer{
		Host:    "0.0.0.0",
		Port:    8888,
		TempDir: "/tmp",

		dbHost:  "127.0.0.1",
		dbPort:  5656,
		dbUser:  "sys",
		dbPass:  "manager",
		dbTable: "trip",
	}
	flag.StringVar(&s.Host, "http-host", s.Host, "the host to bind the HTTP server to")
	flag.IntVar(&s.Port, "http-port", s.Port, "the port to bind the HTTP server to")
	flag.IntVar(&s.KeepAlive, "http-keepalive", 60, "the keep-alive period in seconds for HTTP connections")
	flag.StringVar(&s.TempDir, "http-tempdir", s.TempDir, "the temporary directory for file uploads")

	flag.StringVar(&s.dbHost, "db-host", s.dbHost, "Database host")
	flag.IntVar(&s.dbPort, "db-port", s.dbPort, "Database port")
	flag.StringVar(&s.dbUser, "db-user", s.dbUser, "Database user")
	flag.StringVar(&s.dbPass, "db-pass", s.dbPass, "Database password")
	flag.StringVar(&s.dbTable, "db-table", s.dbTable, "Database table name")

	return s
}

func (s *HttpServer) Featured() {
	feature.Add(s)
}

func (s *HttpServer) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	s.actorSystem = actorSystem
	s.log = actorSystem.Logger().(*util.Log)
	s.log.Printf("Starting HTTP server on %s:%d", s.Host, s.Port)
	return s.serve()
}

func (s *HttpServer) Stop(ctx context.Context) error {
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
