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

type Worker struct{}

func (w *Worker) PreStart(ctx *actor.Context) error { return nil }
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
		prog := &Progress{Src: req.Src, State: int32(WorkStateError), Message: err.Error()}
		ctx.Tell(ctx.Sender(), prog)
		ctx.Err(err)
	}

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

	converters := make([]func(string) any, len(cols))
	for i, col := range cols {
		switch col.Type {
		case api.ColumnTypeShort, api.ColumnTypeUShort, api.ColumnTypeInteger, api.ColumnTypeUInteger:
			converters[i] = func(s string) any { v, _ := strconv.Atoi(s); return v }
		case api.ColumnTypeLong, api.ColumnTypeULong:
			converters[i] = func(s string) any { v, _ := strconv.ParseInt(s, 10, 64); return v }
		case api.ColumnTypeFloat, api.ColumnTypeDouble:
			converters[i] = func(s string) any { v, _ := strconv.ParseFloat(s, 64); return v }
		case api.ColumnTypeVarchar, api.ColumnTypeText:
			converters[i] = func(s string) any { return s }
		case api.ColumnTypeDatetime:
			converters[i] = func(s string) any { v, _ := strconv.ParseInt(s, 10, 64); return v }
		case api.ColumnTypeIPv4:
			converters[i] = func(s string) any { return s }
		case api.ColumnTypeIPv6:
			converters[i] = func(s string) any { return s }
		case api.ColumnTypeJSON:
			converters[i] = func(s string) any { return s }
		case api.ColumnTypeUnknown:
			converters[i] = func(s string) any { return s }
		}
	}

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
			values[i] = converters[i](field)
		}

		if err := appender.Append(values...); err != nil {
			appender.Close()
			replyError(err)
			return
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
