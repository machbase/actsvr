package trjd

import (
	"actsvr/feature"
	"actsvr/server"
	"actsvr/util"
	"context"
	"flag"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

// Trj Loader is a tool to import trip data into Machbase.
//
// Usage:
//
//	trjld [options] <file1> <file2> ...
//
// e.g.
//
//	trjld -db-host 192.168.0.207 -db-port 5656 \
//	      ./data1/CN7_2023-04-06_15-57-39.CSV ./data1/CN7_2023-04-07_09-16-36.CSV
func LoaderMain() int {
	runner := NewRunner()

	master := NewMaster(Config{})
	feature.Add(master)

	ctx := context.Background()
	svr := server.NewServer()
	if err := svr.Serve(ctx); err != nil {
		panic(err)
	}

	for _, a := range flag.Args() {
		stat, err := os.Stat(a)
		if err != nil {
			panic(err)
		}
		if stat.IsDir() {
			panic("directory is not supported: " + a)
		}
		runner.files = append(runner.files, a)
		runner.workers[a] = NewWorkId()
	}

	_, err := svr.ActorSystem().Spawn(ctx, "runner", runner, actor.WithLongLived())
	if err != nil {
		panic(err)
	}
	runner.Wait()

	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	return 0
}

type WorkId string

func NewWorkId() WorkId {
	// Generate UUID v6
	id, err := uuid.NewV6()
	if err != nil {
		// Fallback to v4 if v6 fails
		return WorkId(uuid.New().String())
	}
	return WorkId(id.String())
}

func (w WorkId) String() string {
	return string(w)
}

func (r *ImportRequest) NewProgress(state WorkState) *ImportProgress {
	return &ImportProgress{
		WorkId: r.WorkId,
		Src:    r.Src,
		State:  int32(state),
	}
}

type Runner struct {
	dbHost  string
	dbPort  int
	dbUser  string
	dbPass  string
	dbTable string

	files    []string
	workers  map[string]WorkId
	workerWg sync.WaitGroup
	startWg  chan struct{}
	log      *util.Log
}

func NewRunner() *Runner {
	runner := &Runner{
		dbHost:  "127.0.0.1",
		dbPort:  5656,
		dbUser:  "sys",
		dbPass:  "manager",
		dbTable: "trip",
		files:   make([]string, 0, len(os.Args)-1),
		workers: make(map[string]WorkId),
		startWg: make(chan struct{}),
	}
	flag.StringVar(&runner.dbHost, "db-host", runner.dbHost, "Database host")
	flag.IntVar(&runner.dbPort, "db-port", runner.dbPort, "Database port")
	flag.StringVar(&runner.dbUser, "db-user", runner.dbUser, "Database user")
	flag.StringVar(&runner.dbPass, "db-pass", runner.dbPass, "Database password")
	flag.StringVar(&runner.dbTable, "db-table", runner.dbTable, "Database table name")

	return runner
}

func (c *Runner) Wait() {
	<-c.startWg
	c.workerWg.Wait()
	c.log.Println("All imports completed.")
}

func (c *Runner) PreStart(ctx *actor.Context) error {
	c.log = ctx.ActorSystem().Logger().(*util.Log)
	return nil
}

func (c *Runner) PostStop(ctx *actor.Context) error {
	return nil
}

func (c *Runner) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		for file, workerId := range c.workers {
			c.log.Println("Importing file:", file, "with worker ID:", workerId)
			req := &ImportRequest{
				WorkId:   workerId.String(),
				Src:      file,
				DstHost:  c.dbHost,
				DstPort:  int32(c.dbPort),
				DstUser:  c.dbUser,
				DstPass:  c.dbPass,
				DstTable: c.dbTable,
			}
			ctx.SendAsync(MasterName, req)
			c.workerWg.Add(1)
		}
		close(c.startWg)
	case *ImportProgress:
		switch WorkState(msg.State) {
		case WorkStateIdle:
			c.log.Println("Import starts for:", msg.Src)
		case WorkStateProgress:
			c.log.Printf("Import in progress for: %s, Progress: %.2f%%", msg.Src, msg.Progress*100)
		case WorkStateDone:
			c.log.Println("Import done for:", msg.Src, "Success:", msg.Success, "Fail:", msg.Fail)
			c.workerWg.Done()
		case WorkStateError:
			c.log.Println("Import error for:", msg.Src, "Message:", msg.Message)
			c.workerWg.Done()
		}
	default:
		ctx.Unhandled()
	}
}
