package util

import (
	"fmt"
	"io"
	golog "log"
	"os"
	"time"

	"github.com/tochemey/goakt/v3/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogConfig struct {
	Filename   string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
	UTC        bool
	Timeformat string
	Append     bool
	Verbose    int
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		Filename:   "-",
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
		Compress:   false,
		UTC:        false, // false: local time, true: UTC
		Timeformat: "2006-01-02 15:04:05.000",
		Append:     false, // false: overwrite, true: append
		Verbose:    0,     // 0: no debug, 1: info 2: debug
	}
}

type Log struct {
	w          io.Writer
	tz         *time.Location
	timeformat string
	verbose    int
}

func NewLog(cfg LogConfig) *Log {
	var w io.Writer
	switch cfg.Filename {
	case "-":
		w = os.Stdout
	case "":
		w = io.Discard
	default:
		lj := &lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
			LocalTime:  true,
		}
		if !cfg.Append {
			lj.Rotate()
		}
		w = lj
	}
	tz := time.Local
	if cfg.UTC {
		tz = time.UTC
	}
	return &Log{
		w:          w,
		tz:         tz,
		timeformat: cfg.Timeformat,
		verbose:    cfg.Verbose,
	}
}

var _ log.Logger = (*Log)(nil)

var levels = []string{
	"INFO ",
	"WARN ",
	"ERROR",
	"FATAL",
	"PANIC",
	"DEBUG",
	"-----",
}

const LogID = "util.logger"

func (l *Log) ID() string {
	return LogID
}

func (l *Log) write(level log.Level, msg string) {
	ts := time.Now().In(l.tz).Format(l.timeformat)
	fmt.Fprintln(l.w, ts, levels[level], msg)
}

func (l *Log) Println(args ...any) {
	l.write(log.InvalidLevel, fmt.Sprint(args...))
}

func (l *Log) Printf(f string, args ...any) {
	l.write(log.InvalidLevel, fmt.Sprintf(f, args...))
}

// Info starts a new message with info level.
func (l *Log) Info(args ...any) {
	if l.verbose > 0 {
		l.write(log.InfoLevel, fmt.Sprint(args...))
	}
}

// Infof starts a new message with info level.
func (l *Log) Infof(f string, args ...any) {
	if l.verbose > 0 {
		l.write(log.InfoLevel, fmt.Sprintf(f, args...))
	}
}

// Warn starts a new message with warn level.
func (l *Log) Warn(args ...any) {
	l.write(log.WarningLevel, fmt.Sprint(args...))
}

// Warnf starts a new message with warn level.
func (l *Log) Warnf(f string, args ...any) {
	l.write(log.WarningLevel, fmt.Sprintf(f, args...))
}

// Error starts a new message with error level.
func (l *Log) Error(args ...any) {
	l.write(log.ErrorLevel, fmt.Sprint(args...))
}

// Errorf starts a new message with error level.
func (l *Log) Errorf(f string, args ...any) {
	l.write(log.ErrorLevel, fmt.Sprintf(f, args...))
}

// Fatal starts a new message with fatal level. The os.Exit(1) function
// is called which terminates the program immediately.
func (l *Log) Fatal(args ...any) {
	l.write(log.FatalLevel, fmt.Sprint(args...))
}

// Fatalf starts a new message with fatal level. The os.Exit(1) function
// is called which terminates the program immediately.
func (l *Log) Fatalf(f string, args ...any) {
	l.write(log.FatalLevel, fmt.Sprintf(f, args...))
}

// Panic starts a new message with panic level. The panic() function
// is called which stops the ordinary flow of a goroutine.
func (l *Log) Panic(args ...any) {
	l.write(log.PanicLevel, fmt.Sprint(args...))
}

// Panicf starts a new message with panic level. The panic() function
// is called which stops the ordinary flow of a goroutine.
func (l *Log) Panicf(f string, args ...any) {
	l.write(log.PanicLevel, fmt.Sprintf(f, args...))
}

// Debug starts a new message with debug level.
func (l *Log) Debug(args ...any) {
	if l.verbose > 1 {
		l.write(log.DebugLevel, fmt.Sprint(args...))
	}
}

// Debugf starts a new message with debug level.
func (l *Log) Debugf(f string, args ...any) {
	if l.verbose > 1 {
		l.write(log.DebugLevel, fmt.Sprintf(f, args...))
	}
}

// LogLevel returns the log level being used
func (l *Log) LogLevel() log.Level {
	switch l.verbose {
	default:
		return log.WarningLevel
	case 1:
		return log.InfoLevel
	case 2:
		return log.DebugLevel
	}
}

// LogOutput returns the log output that is set
func (l *Log) LogOutput() []io.Writer {
	return []io.Writer{l.w}
}

// StdLogger returns the standard logger associated to the logger
func (l *Log) StdLogger() *golog.Logger {
	return golog.New(os.Stdout, "", golog.LstdFlags)
}
