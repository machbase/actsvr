package siotsvr

import (
	"actsvr/server"
	"actsvr/util"
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

var DefaultLocation = time.Local
var pid string = "./siotsvr.pid"
var logger *util.Log

var machConfig = MachConfig{
	dbHost: "127.0.0.1",
	dbPort: 5656,
	dbUser: "sys",
	dbPass: "manager",
}

var rdbConfig = RDBConfig{
	host: "siot.redirectme.net",
	port: 3306,
	user: "iotdb",
	pass: "iotmanager",
	db:   "iotdb",
}

func Main() int {
	flag.StringVar(&pid, "pid", pid, "the file to store the process ID")

	flag.StringVar(&machConfig.dbHost, "db-host", machConfig.dbHost, "Database host")
	flag.IntVar(&machConfig.dbPort, "db-port", machConfig.dbPort, "Database port")
	flag.StringVar(&machConfig.dbUser, "db-user", machConfig.dbUser, "Database user")
	flag.StringVar(&machConfig.dbPass, "db-pass", machConfig.dbPass, "Database password")

	flag.StringVar(&rdbConfig.host, "rdb-host", rdbConfig.host, "RDB host")
	flag.IntVar(&rdbConfig.port, "rdb-port", rdbConfig.port, "RDB port")
	flag.StringVar(&rdbConfig.user, "rdb-user", rdbConfig.user, "RDB user")
	flag.StringVar(&rdbConfig.pass, "rdb-pass", rdbConfig.pass, "RDB password")
	flag.StringVar(&rdbConfig.db, "rdb-db", rdbConfig.db, "RDB database")

	poiSvr := NewPoiServer()
	poiSvr.Featured()
	httpSvr := NewHttpServer()
	httpSvr.Featured()

	poiGroup := httpSvr.Router().Group("/db/poi")
	poiSvr.Router(poiGroup)

	ctx := context.Background()
	svr := server.NewServer()
	if err := svr.Serve(ctx); err != nil {
		panic(err)
	}

	logger = svr.ActorSystem().Logger().(*util.Log)
	if pid != "" {
		if err := os.WriteFile(pid, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			logger.Printf("failed to write PID file: %v", err)
			panic(fmt.Errorf("failed to write PID file: %w", err))
		}
		defer func() {
			if err := os.Remove(pid); err != nil {
				logger.Printf("Failed to remove PID file %s: %v", pid, err)
			}
		}()
	}

	svr.WaitInterrupt()
	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	return 0
}

type MachConfig struct {
	dbHost string
	dbPort int
	dbUser string
	dbPass string
}

type RDBConfig struct {
	host string
	port int
	user string
	pass string
	db   string
}
