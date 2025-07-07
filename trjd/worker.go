package trjd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/tochemey/goakt/v3/actor"
)

type WorkState int

const (
	WorkStateIdle WorkState = iota
	WorkStateRunning
	WorkStateDone
	WorkStateError
)

type Worker struct {
}

func (w *Worker) PreStart(ctx *actor.Context) error { return nil }
func (w *Worker) PostStop(ctx *actor.Context) error { return nil }

func (w *Worker) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *ImportRequest:
		w.doImport(ctx, msg)
	default:
		ctx.Unhandled()
	}
}

func (w *Worker) doImport(ctx *actor.ReceiveContext, req *ImportRequest) {
	data, err := os.Open(req.Src)
	if err != nil {
		ctx.Err(err)
		return
	}
	defer data.Close()

	fileInfo, _ := data.Stat()
	fileSize := fileInfo.Size()
	progReader := NewProgressReader(data, fileSize)

	// machcli database
	db, err := machcli.NewDatabase(&machcli.Config{
		Host:         req.DstHost,
		Port:         int(req.DstPort),
		TrustUsers:   map[string]string{req.DstUser: req.DstPass},
		MaxOpenConn:  -1,
		MaxOpenQuery: -1,
	})
	if err != nil {
		ctx.Err(err)
		return
	}
	defer db.Close()

	conn, err := db.Connect(ctx.Context(), api.WithPassword(req.DstUser, req.DstPass))
	if err != nil {
		ctx.Err(err)
		return
	}
	defer conn.Close()

	appender, err := conn.Appender(ctx.Context(), req.DstTable)
	if err != nil {
		ctx.Err(err)
		return
	}
	defer func() {
		succ, fail, err := appender.Close()
		if err != nil {
			ctx.Err(err)
			return
		}
		if prog := req.NewProgress(WorkStateDone); prog != nil {
			prog.Progress = 1.0
			prog.Success = succ
			prog.Fail = fail
			ctx.Tell(ctx.Sender(), prog)
		}
	}()

	if prog := req.NewProgress(WorkStateIdle); prog != nil {
		ctx.Tell(ctx.Sender(), prog)
	}
	if err := w.processImport(req.Src, progReader, appender); err != nil {
		if prog := req.NewProgress(WorkStateError); prog != nil {
			prog.Message = fmt.Sprintf("Worker %s error %v", ctx.Self().Name(), err)
			ctx.Tell(ctx.Sender(), prog)
		}
		ctx.Err(err)
		return
	}
}

func (w *Worker) processImport(inputFile string, progReader *ProgressReader, appender api.Appender) error {
	// Decide the trip ID from the file name
	var tripId = strings.TrimSuffix(strings.ToUpper(filepath.Base(inputFile)), ".CSV")
	var tripStartTime time.Time
	var isCanData bool = strings.HasSuffix(strings.ToLower(inputFile), ".can")

	if !isCanData {
		buff := &bytes.Buffer{}
		// read the first line which contains the trip start time
		// ex) Date:06.04.2023 Time:06:57:39
		for {
			var char [1]byte
			if n, err := progReader.Read(char[:]); err != nil || n == 0 {
				return fmt.Errorf("invalid format or empty file")
			}
			if char[0] == '\n' {
				break
			} else {
				buff.WriteByte(char[0])
			}
		}

		// parse start time
		var startTimeRegex = regexp.MustCompile(`Date:(\d{2})\.(\d{2})\.(\d{4}) Time:(\d{2}):(\d{2}):(\d{2})`)
		if match := startTimeRegex.FindStringSubmatch(buff.String()); match != nil {
			day, _ := strconv.ParseInt(match[1], 10, 32)
			month, _ := strconv.ParseInt(match[2], 10, 32)
			year, _ := strconv.ParseInt(match[3], 10, 32)
			hours, _ := strconv.ParseInt(match[4], 10, 32)
			minutes, _ := strconv.ParseInt(match[5], 10, 32)
			seconds, _ := strconv.ParseInt(match[6], 10, 32)

			tripStartTime = time.Date(int(year), time.Month(month), int(day), int(hours), int(minutes), int(seconds), 0, time.Local)
		} else {
			return fmt.Errorf("invalid format - no date line, it might be CAN data")
		}
		buff.Reset()
	} else {
		fmt.Printf("ID: %s\n", tripId)
	}

	csvReader := csv.NewReader(progReader)

	var headerTrimRegex = regexp.MustCompile(`\s*\[.*\]$`)
	var headers = []string{}
	var headerIndex = map[string]int{}

	// parse header line
	fields, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("error reading header line: %w", err)
	}
	for idx, h := range fields {
		if h == "" { // all lines contains an empty field at the end
			continue
		}
		name := headerTrimRegex.ReplaceAllString(h, "")
		headers = append(headers, name)
		headerIndex[name] = idx
	}

	// parse body lines
	for {
		fields, err := csvReader.Read()
		if err != nil {
			break
		}
		rec := NewRecord(headers, fields)
		var timestamp int64
		var value = 0.0
		if !isCanData { // CN7, RG3
			// TIME
			timestamp = tripStartTime.UnixNano() + int64(rec["t"].(float64)*1000000000)
			// VALUE
			if v, ok := rec["Speed_Kmh"]; ok {
				value = v.(float64)
			}
		} else { // CAN - raw & interpolated
			// TIME
			timestamp = int64(rec["timestamps"].(float64)) * 1000000
			// VALUE
			if v, ok := rec["WHL_SpdFLVal"]; ok {
				value = v.(float64)
			}
		}
		// DATA
		jsonData, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("error marshalling record: %w", err)
		}
		escaped := strings.ReplaceAll(string(jsonData), `"`, `""`)
		ts := time.Unix(0, timestamp)
		if err := appender.Append(tripId, ts, value, escaped); err != nil {
			return err
		}
	}
	return nil
}

type Record map[string]any

func NewRecord(headers, fields []string) Record {
	r := Record{}
	for idx, h := range headers {
		if h == "" { // all lines contains empty string at the end
			continue
		}
		if v := fields[idx]; len(v) > 0 {
			r[h], _ = strconv.ParseFloat(v, 64)
		}
	}
	return r
}
