package main

import (
	"actsvr/feature"
	"actsvr/server"
	"actsvr/trjd"
	"context"
	"flag"
	"fmt"
	"os"
	"sync"

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
func main() {
	runner := NewRunner()

	master := trjd.NewMaster(trjd.Config{})
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
		runner.workers[a] = trjd.NewWorkId()
	}

	_, err := svr.ActorSystem().Spawn(ctx, "runner", runner, actor.WithLongLived())
	if err != nil {
		panic(err)
	}
	runner.Wait()

	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	os.Exit(0)
}

type Runner struct {
	dbHost  string
	dbPort  int
	dbUser  string
	dbPass  string
	dbTable string

	files    []string
	workers  map[string]trjd.WorkId
	workerWg sync.WaitGroup
	startWg  chan struct{}
}

func NewRunner() *Runner {
	runner := &Runner{
		dbHost:  "127.0.0.1",
		dbPort:  5656,
		dbUser:  "sys",
		dbPass:  "manager",
		dbTable: "trip",
		files:   make([]string, 0, len(os.Args)-1),
		workers: make(map[string]trjd.WorkId),
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
	fmt.Println("All imports completed.")
}

func (c *Runner) PreStart(ctx *actor.Context) error { return nil }
func (c *Runner) PostStop(ctx *actor.Context) error { return nil }

func (c *Runner) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		for file, workerId := range c.workers {
			fmt.Println("-------- Importing file:", file, "with worker ID:", workerId)
			req := &trjd.ImportRequest{
				WorkId:   workerId.String(),
				Src:      file,
				DstHost:  c.dbHost,
				DstPort:  int32(c.dbPort),
				DstUser:  c.dbUser,
				DstPass:  c.dbPass,
				DstTable: c.dbTable,
			}
			ctx.SendAsync(trjd.MasterName, req)
			c.workerWg.Add(1)
		}
		close(c.startWg)
	case *trjd.ImportProgress:
		switch trjd.WorkState(msg.State) {
		case trjd.WorkStateIdle:
			fmt.Println("-------- Starting import for:", msg.Src)
		case trjd.WorkStateRunning:
			fmt.Printf("-------- Import in progress for: %s, Progress: %.2f%%\n", msg.Src, msg.Progress*100)
		case trjd.WorkStateDone:
			fmt.Println("-------- Import done for:", msg.Src, "Success:", msg.Success, "Fail:", msg.Fail)
			c.workerWg.Done()
		case trjd.WorkStateError:
			fmt.Println("-------- Import error for:", msg.Src, "Message:", msg.Message)
			c.workerWg.Done()
		}
	default:
		ctx.Unhandled()
	}
}
