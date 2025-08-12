package siotsvr

import (
	"actsvr/util"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
)

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds
	TempDir   string

	machCli       *machcli.Database
	log           *util.Log
	httpServer    *http.Server
	router        *gin.Engine
	certKeys      map[string]*Certkey
	certKeysMutex sync.RWMutex
}

func NewHttpServer() *HttpServer {
	return &HttpServer{
		Host:    "0.0.0.0",
		Port:    8888,
		TempDir: "/tmp",
	}
}

func (s *HttpServer) Start(ctx context.Context) error {
	s.log = DefaultLog()
	if err := s.reloadCertkey(); err != nil {
		return err
	}
	if err := s.openDatabase(); err != nil {
		return err
	}
	s.log.Infof("Starting HTTP server on %s:%d", s.Host, s.Port)
	return s.serve()
}

func (s *HttpServer) Stop(ctx context.Context) error {
	s.closeDatabase()
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

func (s *HttpServer) reloadCertkey() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	lst := map[string]*Certkey{}
	err = SelectCertkey(rdb, func(certkey *Certkey) bool {
		lst[certkey.CrtfcKey.String] = certkey
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Certkey: %w", err)
	}
	s.certKeysMutex.Lock()
	s.certKeys = lst
	s.log.Infof("Loaded %d certificate keys", len(lst))
	s.certKeysMutex.Unlock()
	return nil
}
