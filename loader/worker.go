package loader

import (
	"actsvr/util"
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/tochemey/goakt/v3/actor"
)

type State int

const (
	WorkStateIdle State = iota
	WorkStateProgress
	WorkStateDone
	WorkStateError
)

type Config struct {
	DstHost          string
	DstPort          int
	DstUser          string
	DstPass          string
	DstTable         string
	ColumnNamesFile  string         // file containing column names for the CSV file
	ProgressInterval time.Duration  //
	Timeformat       string         // e.g., "ns", "us", "ms", "s", "2006-01-02 15:04:05"
	Timezone         string         // e.g., "UTC",
	SkipHeader       bool           // whether to skip the first line (header) in CSV files
	DelayForTest     time.Duration  // for testing purposes, in nanoseconds
	tz               *time.Location // Timezone for parsing datetime fields
}

func NewConfig() *Config {
	return &Config{
		DstHost:          "127.0.0.1",
		DstPort:          5656,
		DstUser:          "sys",
		DstPass:          "manager",
		DstTable:         "",
		ProgressInterval: 1 * time.Second,
	}
}

func (c *Config) NewWorker(input string) *Worker {
	var err error
	c.tz = time.Local
	if c.Timezone != "" {
		c.tz, err = time.LoadLocation(c.Timezone)
		if err != nil {
			panic("Invalid timezone: " + c.Timezone)
		}
	}
	return &Worker{
		conf:  c,
		input: input,
	}
}

type Worker struct {
	log   *util.Log
	conf  *Config
	input string
}

func (w *Worker) PreStart(ctx *actor.Context) error {
	w.log = ctx.ActorSystem().Logger().(*util.Log)
	return nil
}

func (w *Worker) PostStop(ctx *actor.Context) error { return nil }

func (w *Worker) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *Request:
		w.doImport(ctx)
	default:
		ctx.Unhandled()
	}
}

func (w *Worker) doImport(ctx *actor.ReceiveContext) {
	replyError := func(err error) {
		w.log.Errorf("Worker %s error: %v", ctx.Self().Name(), err)
		prog := &Progress{Src: w.input, State: int32(WorkStateError), Message: err.Error()}
		ctx.Tell(ctx.Sender(), prog)
		ctx.Err(err)
	}

	data, err := os.Open(w.input)
	if err != nil {
		replyError(err)
		return
	}
	defer data.Close()

	fileInfo, _ := data.Stat()
	fileSize := fileInfo.Size()
	progReader := util.NewProgressReader(data, fileSize)
	defer progReader.Close()

	// column names file, it contains the column names for the CSV file, one name per line
	columnOrder := make([]string, 0)
	if w.conf.ColumnNamesFile != "" {
		columnNamesFile, err := os.Open(w.conf.ColumnNamesFile)
		if err != nil {
			replyError(err)
			return
		}
		scanner := bufio.NewScanner(columnNamesFile)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue // skip empty lines
			}
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue // skip empty lines
			}
			columnOrder = append(columnOrder, strings.ToUpper(fields[0]))
		}
		if err := scanner.Err(); err != nil {
			replyError(err)
			return
		}
		columnNamesFile.Close()
	}

	// machcli database
	db, err := machcli.NewDatabase(&machcli.Config{
		Host:         w.conf.DstHost,
		Port:         int(w.conf.DstPort),
		TrustUsers:   map[string]string{w.conf.DstUser: w.conf.DstPass},
		MaxOpenConn:  -1,
		MaxOpenQuery: -1,
	})
	if err != nil {
		replyError(err)
		return
	}
	defer db.Close()

	conn, err := db.Connect(ctx.Context(), api.WithPassword(w.conf.DstUser, w.conf.DstPass))
	if err != nil {
		replyError(err)
		return
	}
	defer conn.Close()

	appender, err := conn.Appender(ctx.Context(), w.conf.DstTable)
	if err != nil {
		replyError(err)
		return
	}
	cols, err := appender.Columns()
	if err != nil {
		appender.Close()
		replyError(err)
		return
	}
	if len(cols) > 0 && cols[0].Name == "_ARRIVAL_TIME" &&
		len(columnOrder) > 0 && columnOrder[0] != "_ARRIVAL_TIME" {
		cols = cols[1:] // skip _ARRIVAL_TIME column
	}
	converters := w.buildConverters(cols)

	var columnOrderIndexes []int
	if len(columnOrder) > 0 {
		columnOrderIndexes = make([]int, len(cols))
		for i, col := range cols {
			idx := -1
			for j, name := range columnOrder {
				if name == col.Name {
					idx = j
					break
				}
			}
			columnOrderIndexes[i] = idx
		}
	}

	prog := &Progress{Src: w.input, State: int32(WorkStateIdle)}
	ctx.Tell(ctx.Sender(), prog)

	now := time.Now()
	csvReader := csv.NewReader(progReader)
	shouleSkipHeader := w.conf.SkipHeader
	progressInterval := w.conf.ProgressInterval
	if progressInterval <= 0 {
		progressInterval = 1 * time.Second // default progress interval
	}
	for {
		fields, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break // end of file
			}
			appender.Close()
			replyError(err)
			break
		}
		if shouleSkipHeader {
			shouleSkipHeader = false
			continue // skip header line
		}
		var values []any
		if len(columnOrderIndexes) > 0 {
			values = make([]any, len(columnOrderIndexes))
			for i, idx := range columnOrderIndexes {
				if idx < 0 || idx >= len(fields) {
					continue
				}
				values[i], err = converters[i](fields[idx])
				if err != nil {
					appender.Close()
					replyError(err)
					return
				}
			}
		} else {
			values = make([]any, len(fields))
			for i, field := range fields {
				values[i], err = converters[i](field)
				if err != nil {
					appender.Close()
					replyError(err)
					return
				}
			}
		}

		if err := appender.Append(values...); err != nil {
			appender.Close()
			replyError(err)
			return
		}
		// simulate some processing delay
		if w.conf.DelayForTest > 0 {
			time.Sleep(w.conf.DelayForTest)
		}

		if ts := time.Now(); ts.Sub(now) > progressInterval {
			now = ts
			prog = &Progress{
				Src:      w.input,
				State:    int32(WorkStateProgress),
				Progress: progReader.Progress(),
			}
			ctx.Tell(ctx.Sender(), prog)
		}
	}

	succ, fail, err := appender.Close()
	if err != nil {
		replyError(err)
		return
	}
	prog = &Progress{Src: w.input, State: int32(WorkStateDone), Success: succ, Fail: fail}
	ctx.Tell(ctx.Sender(), prog)
}

func (w *Worker) buildConverters(cols api.Columns) []func(string) (any, error) {
	converters := make([]func(string) (any, error), len(cols))
	for i, col := range cols {
		switch col.Type {
		case api.ColumnTypeShort, api.ColumnTypeUShort, api.ColumnTypeInteger, api.ColumnTypeUInteger:
			converters[i] = func(s string) (any, error) { v, _ := strconv.Atoi(s); return v, nil }
		case api.ColumnTypeLong, api.ColumnTypeULong:
			converters[i] = func(s string) (any, error) { v, _ := strconv.ParseInt(s, 10, 64); return v, nil }
		case api.ColumnTypeFloat, api.ColumnTypeDouble:
			converters[i] = func(s string) (any, error) { v, _ := strconv.ParseFloat(s, 64); return v, nil }
		case api.ColumnTypeVarchar, api.ColumnTypeText:
			converters[i] = func(s string) (any, error) {
				if s == "" {
					return nil, nil // handle empty string as nil
				}
				return s, nil
			}
		case api.ColumnTypeDatetime:
			converters[i] = func(s string) (any, error) {
				switch w.conf.Timeformat {
				case "s":
					v, _ := strconv.ParseInt(s, 10, 64)
					return v * int64(time.Second), nil
				case "ms":
					v, _ := strconv.ParseInt(s, 10, 64)
					return v * int64(time.Millisecond), nil
				case "us":
					v, _ := strconv.ParseInt(s, 10, 64)
					return v * int64(time.Microsecond), nil
				case "ns":
					v, _ := strconv.ParseInt(s, 10, 64)
					return v, nil
				default:
					if w.conf.Timeformat != "" {
						t, err := time.ParseInLocation(w.conf.Timeformat, s, w.conf.tz)
						if err != nil {
							return nil, err
						}
						return t.UnixNano(), nil
					} else {
						v, _ := strconv.ParseInt(s, 10, 64)
						return v, nil
					}
				}
			}
		case api.ColumnTypeIPv4:
			converters[i] = func(s string) (any, error) { return s, nil }
		case api.ColumnTypeIPv6:
			converters[i] = func(s string) (any, error) { return s, nil }
		case api.ColumnTypeJSON:
			converters[i] = func(s string) (any, error) { return s, nil }
		case api.ColumnTypeUnknown:
			converters[i] = func(s string) (any, error) { return s, nil }
		}
	}
	return converters
}
