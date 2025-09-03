package siotsvr

import (
	"actsvr/util"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var pid string = "./siotsvr.pid"
var defaultLog *util.Log
var nowFunc = time.Now
var packetDataArrivalTime = &LastArrivalTime{Name: "last_packet"}
var parsDataArrivalTime = &LastArrivalTime{Name: "last_pars"}
var arrivalTimeDir string = "."
var arrivalQueryLimit int = 1000
var DefaultTZ *time.Location
var statTagTable string = "TAG"

const MaskingStrValue = "***"

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
	if tz, err := time.LoadLocation("Asia/Seoul"); err != nil {
		panic(fmt.Sprintf("failed to load timezone: %v", err))
	} else {
		DefaultTZ = tz
	}

	httpSvr := NewHttpServer()
	flag.StringVar(&pid, "pid", pid, "the file to store the process ID")
	// Machbase configuration
	flag.StringVar(&machConfig.dbHost, "db-host", machConfig.dbHost, "Database host")
	flag.IntVar(&machConfig.dbPort, "db-port", machConfig.dbPort, "Database port")
	flag.StringVar(&machConfig.dbUser, "db-user", machConfig.dbUser, "Database user")
	flag.StringVar(&machConfig.dbPass, "db-pass", machConfig.dbPass, "Database password")
	flag.StringVar(&arrivalTimeDir, "last-dir", arrivalTimeDir, "the directory to store arrival time files")
	flag.IntVar(&arrivalQueryLimit, "last-limit", arrivalQueryLimit, "the maximum number of rows to query for arrival time update")

	// RDB configuration
	flag.StringVar(&rdbConfig.host, "rdb-host", rdbConfig.host, "RDB host")
	flag.IntVar(&rdbConfig.port, "rdb-port", rdbConfig.port, "RDB port")
	flag.StringVar(&rdbConfig.user, "rdb-user", rdbConfig.user, "RDB user")
	flag.StringVar(&rdbConfig.pass, "rdb-pass", rdbConfig.pass, "RDB password")
	flag.StringVar(&rdbConfig.db, "rdb-db", rdbConfig.db, "RDB database")

	// HTTP server configuration
	flag.StringVar(&httpSvr.Host, "http-host", httpSvr.Host, "the host to bind the HTTP server to")
	flag.IntVar(&httpSvr.Port, "http-port", httpSvr.Port, "the port to bind the HTTP server to")
	flag.IntVar(&httpSvr.KeepAlive, "http-keepalive", 60, "the keep-alive period in seconds for HTTP connections")
	flag.StringVar(&httpSvr.DataDir, "http-datadir", httpSvr.DataDir, "the data directory")

	// replica configuration
	flag.IntVar(&replicaRowsPerRun, "replica-rows", replicaRowsPerRun, "number of rows to process in each replica sync run")

	// logging configuration
	logConf := util.DefaultLogConfig()
	logConf.Timeformat = "2006-01-02 15:04:05.000"
	logConf.UTC = false
	flag.StringVar(&logConf.Filename, "log-filename", logConf.Filename, "the log file name")
	flag.IntVar(&logConf.MaxSize, "log-max-size", logConf.MaxSize, "the maximum size of the log file in megabytes")
	flag.IntVar(&logConf.MaxBackups, "log-max-backups", logConf.MaxBackups, "the maximum number of log file backups")
	flag.IntVar(&logConf.MaxAge, "log-max-age", logConf.MaxAge, "the maximum age of the log file in days")
	flag.BoolVar(&logConf.Compress, "log-compress", logConf.Compress, "whether to compress the log file")
	flag.BoolVar(&logConf.UTC, "log-utc", logConf.UTC, "whether to use local time in the log file")
	flag.BoolVar(&logConf.Append, "log-append", logConf.Append, "whether to append to the log file or overwrite it")
	flag.StringVar(&logConf.Timeformat, "log-timeformat", logConf.Timeformat, "the time format to use in the log file")
	flag.IntVar(&logConf.Verbose, "log-verbose", logConf.Verbose, "0: no debug, 1: info, 2: debug")

	flag.Parse()
	defaultLog = util.NewLog(logConf)

	if pid != "" {
		if err := os.WriteFile(pid, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			log.Printf("failed to write PID file: %v", err)
			panic(fmt.Errorf("failed to write PID file: %w", err))
		}
		defer func() {
			if err := os.Remove(pid); err != nil {
				log.Printf("Failed to remove PID file %s: %v", pid, err)
			}
		}()
	}

	packetDataArrivalTime.Load()
	parsDataArrivalTime.Load()

	collector := Collector()
	collector.Start()

	ctx, ctxCancel := context.WithCancel(context.Background())
	if err := httpSvr.Start(ctx); err != nil {
		log.Printf("Failed to start HttpServer: %v", err)
		ctxCancel()
		return 1
	}

	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	if err := httpSvr.Stop(ctx); err != nil {
		log.Printf("Error stopping HttpServer: %v", err)
	}
	collector.Stop()
	ctxCancel()
	return 0
}

func DefaultLog() *util.Log {
	if defaultLog == nil {
		defaultLog = util.NewLog(util.DefaultLogConfig())
	}
	return defaultLog
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

func (c RDBConfig) Connect() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.user, c.pass, c.host, c.port, c.db)
	rdb, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return rdb, nil
}

type LastArrivalTime struct {
	sync.Mutex
	Name string
	Time time.Time
}

func (lat *LastArrivalTime) Load() {
	path := filepath.Join(arrivalTimeDir, fmt.Sprintf("%s.txt", lat.Name))
	if b, err := os.ReadFile(path); err == nil {
		if t, err := time.ParseInLocation("2006-01-02 15:04:05.000000000", string(b), DefaultTZ); err == nil {
			lat.Time = t
		}
	}
}

func (lat *LastArrivalTime) Save() {
	path := filepath.Join(arrivalTimeDir, fmt.Sprintf("%s.txt", lat.Name))
	content := []byte(lat.Time.In(DefaultTZ).Format("2006-01-02 15:04:05.000000000"))
	if err := os.WriteFile(path, content, 0644); err != nil {
		log.Printf("Failed to save arrival time for %s: %v", lat.Name, err)
	}
}
