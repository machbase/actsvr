package loader

import (
	"actsvr/server"
	"actsvr/util"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	sync "sync"
	"time"

	"fortio.org/progressbar"
	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

// Loader is a tool to import CSV files into Machbase.
//
// Usage:
//
//	loader [options] <file1> <file2> ...
//
// e.g.
//
//	loader \
//	  -db-host 192.168.0.207 \
//	  -db-port 5656 \
//	  -db-table TAG \
//	  -skip-header  \
//	  -timeformat "2006-01-02 15:04:05" \
//	  -tz "Asia/Seoul" \
//	  ./test_data/sample.csv
func Main() int {
	runner := NewRunner()

	ctx := context.Background()
	svr := server.NewServer()
	if err := svr.Serve(ctx); err != nil {
		panic(err)
	}

	if runner.dbTable == "" {
		flag.Usage()
		panic("db-table is required")
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

type Runner struct {
	dbHost       string
	dbPort       int
	dbUser       string
	dbPass       string
	dbTable      string
	skipHeader   bool
	timeformat   string
	tz           string
	delayForTest time.Duration // for testing purposes, in nanoseconds
	silent       bool          // if true, suppresses progress output

	files    []string
	workerWg sync.WaitGroup
	startWg  chan struct{}
	log      *util.Log
	bars     map[string]*progressbar.Bar
	multiBar *progressbar.MultiBar
}

func NewRunner() *Runner {
	runner := &Runner{
		dbHost:     "127.0.0.1",
		dbPort:     5656,
		dbUser:     "sys",
		dbPass:     "manager",
		skipHeader: false,

		files:   make([]string, 0, len(os.Args)-1),
		startWg: make(chan struct{}),
		bars:    make(map[string]*progressbar.Bar),
	}
	flag.StringVar(&runner.dbHost, "db-host", runner.dbHost, "Database host")
	flag.IntVar(&runner.dbPort, "db-port", runner.dbPort, "Database port")
	flag.StringVar(&runner.dbUser, "db-user", runner.dbUser, "Database user")
	flag.StringVar(&runner.dbPass, "db-pass", runner.dbPass, "Database password")
	flag.StringVar(&runner.dbTable, "db-table", runner.dbTable, "Database table name")
	flag.BoolVar(&runner.skipHeader, "skip-header", runner.skipHeader, "Skip the first line of the CSV file (header)")
	flag.StringVar(&runner.timeformat, "timeformat", "ns", "Time format for the CSV file, e.g., 'ns', 'us', 'ms', 's', '2006-01-02 15:04:05'")
	flag.StringVar(&runner.tz, "tz", "Local", "Time zone for the CSV file, e.g., 'UTC', 'Local', 'Asia/Seoul'")
	flag.BoolVar(&runner.silent, "silent", false, "If true, suppresses progress output")
	flag.DurationVar(&runner.delayForTest, "delay", 0, "Delay for testing purposes")
	return runner
}

func (c *Runner) Wait() {
	<-c.startWg
	c.workerWg.Wait()
	c.log.Println("End")
}

func (c *Runner) PreStart(ctx *actor.Context) error {
	c.log = ctx.ActorSystem().Logger().(*util.Log)
	return nil
}
func (c *Runner) PostStop(ctx *actor.Context) error {
	if c.multiBar != nil {
		c.multiBar.End()
	}
	return nil
}

func (c *Runner) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		cfg := progressbar.DefaultConfig()
		cfg.ExtraLines = 0
		cfg.ScreenWriter = os.Stdout
		if !c.silent {
			c.multiBar = &progressbar.MultiBar{Config: cfg}
		}

		for i, file := range c.files {
			workerId := fmt.Sprintf("worker-%d", i+1)
			c.log.Println("Importing ", workerId, " ", file)

			if !c.silent {
				cfg.Prefix = filepath.Base(file)
				bar := cfg.NewBar()
				c.multiBar.Add(bar)
				c.bars[file] = bar
			}

			worker := &Worker{}
			req := &Request{
				Src:          file,
				DstHost:      c.dbHost,
				DstPort:      int32(c.dbPort),
				DstUser:      c.dbUser,
				DstPass:      c.dbPass,
				DstTable:     c.dbTable,
				SkipHeader:   c.skipHeader,
				Timeformat:   c.timeformat,
				Timezone:     c.tz,
				DelayForTest: int64(c.delayForTest),
			}
			wpid := ctx.Spawn(workerId, worker, actor.WithLongLived())
			ctx.Watch(wpid)
			ctx.Tell(wpid, req)
			c.workerWg.Add(1)
		}
		if !c.silent {
			c.multiBar.PrefixesAlign()
		}
		close(c.startWg)
	case *goaktpb.Terminated:
		c.log.Println("Worker terminated:", msg.ActorId)
	case *Progress:
		switch State(msg.State) {
		case WorkStateIdle:
			c.log.Println(msg.Src, " starts")
		case WorkStateProgress:
			if c.silent {
				c.log.Printf("%s progress: %.2f%%", msg.Src, msg.Progress*100)
			} else {
				c.bars[msg.Src].Progress(msg.Progress * 100)
			}
		case WorkStateDone:
			if c.silent {
				c.log.Println(msg.Src, " done.", " success:", msg.Success, ", fail:", msg.Fail)
			} else {
				c.bars[msg.Src].Progress(100)
			}
			c.workerWg.Done()
		case WorkStateError:
			c.log.Println(msg.Src, " ERROR: ", msg.Message)
			c.workerWg.Done()
		}
	default:
		ctx.Unhandled()
	}
}
