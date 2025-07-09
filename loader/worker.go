package loader

import (
	"actsvr/util"
	"encoding/csv"
	"io"
	"os"
	"strconv"
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

type Worker struct {
	timeformat   string
	tz           string
	skipHeader   bool
	delayForTest time.Duration
	log          *util.Log
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
	req := ctx.Message().(*Request)
	replyError := func(err error) {
		w.log.Errorf("Worker %s error: %v", ctx.Self().Name(), err)
		prog := &Progress{Src: req.Src, State: int32(WorkStateError), Message: err.Error()}
		ctx.Tell(ctx.Sender(), prog)
		ctx.Err(err)
	}
	w.timeformat = req.Timeformat
	w.skipHeader = req.SkipHeader
	w.tz = req.Timezone
	w.delayForTest = time.Duration(req.DelayForTest)

	data, err := os.Open(req.Src)
	if err != nil {
		replyError(err)
		return
	}
	defer data.Close()

	fileInfo, _ := data.Stat()
	fileSize := fileInfo.Size()
	progReader := util.NewProgressReader(data, fileSize)
	defer progReader.Close()

	// machcli database
	db, err := machcli.NewDatabase(&machcli.Config{
		Host:         req.DstHost,
		Port:         int(req.DstPort),
		TrustUsers:   map[string]string{req.DstUser: req.DstPass},
		MaxOpenConn:  -1,
		MaxOpenQuery: -1,
	})
	if err != nil {
		replyError(err)
		return
	}
	defer db.Close()

	conn, err := db.Connect(ctx.Context(), api.WithPassword(req.DstUser, req.DstPass))
	if err != nil {
		replyError(err)
		return
	}
	defer conn.Close()

	appender, err := conn.Appender(ctx.Context(), req.DstTable)
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
	converters := w.buildConverters(cols)

	prog := &Progress{Src: req.Src, State: int32(WorkStateIdle)}
	ctx.Tell(ctx.Sender(), prog)

	now := time.Now()
	csvReader := csv.NewReader(progReader)
	shouleSkipHeader := req.SkipHeader
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
		values := make([]any, len(fields))
		for i, field := range fields {
			values[i], err = converters[i](field)
			if err != nil {
				appender.Close()
				replyError(err)
				return
			}
		}

		if err := appender.Append(values...); err != nil {
			appender.Close()
			replyError(err)
			return
		}
		// simulate some processing delay
		if w.delayForTest > 0 {
			time.Sleep(w.delayForTest)
		}

		if ts := time.Now(); ts.Sub(now) > 1*time.Second {
			now = ts
			prog = &Progress{
				Src:      req.Src,
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
	prog = &Progress{Src: req.Src, State: int32(WorkStateDone), Success: succ, Fail: fail}
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
			converters[i] = func(s string) (any, error) { return s, nil }
		case api.ColumnTypeDatetime:
			converters[i] = func(s string) (any, error) {
				switch w.timeformat {
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
					if w.timeformat != "" {
						t, err := time.ParseInLocation(w.timeformat, s, time.Local)
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
