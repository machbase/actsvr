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

	runner.Start(ctx, svr.ActorSystem())
	runner.Wait()

	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}
	return 0
}

type Runner struct {
	Conf   *Config
	silent bool // if true, suppresses progress output

	files    []string
	workerWg sync.WaitGroup
	startWg  chan struct{}
	log      *util.Log
	bars     map[string]*progressbar.Bar
	multiBar *progressbar.MultiBar
}

func NewRunner() *Runner {
	r := &Runner{
		Conf:    NewConfig(),
		silent:  false,
		files:   make([]string, 0, len(os.Args)-1),
		startWg: make(chan struct{}),
		bars:    make(map[string]*progressbar.Bar),
	}

	conf := r.Conf
	flag.StringVar(&conf.DstHost, "db-host", conf.DstHost, "Database host")
	flag.IntVar(&conf.DstPort, "db-port", conf.DstPort, "Database port")
	flag.StringVar(&conf.DstUser, "db-user", conf.DstUser, "Database user")
	flag.StringVar(&conf.DstPass, "db-pass", conf.DstPass, "Database password")
	flag.StringVar(&conf.DstTable, "db-table", conf.DstTable, "Database table name")
	flag.BoolVar(&conf.SkipHeader, "skip-header", conf.SkipHeader, "Skip the first line of the CSV file (header)")
	flag.StringVar(&conf.Timeformat, "timeformat", "ns", "Time format for the CSV file, e.g., 'ns', 'us', 'ms', 's', '2006-01-02 15:04:05'")
	flag.StringVar(&conf.Timezone, "tz", "Local", "Time zone for the CSV file, e.g., 'UTC', 'Local', 'Asia/Seoul'")
	flag.DurationVar(&conf.DelayForTest, "delay", 0, "Delay for testing purposes")
	flag.BoolVar(&r.silent, "silent", false, "If true, suppresses progress output")

	return r
}

func (r *Runner) Start(ctx context.Context, actorSystem actor.ActorSystem) {
	if r.Conf.DstTable == "" {
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
		r.files = append(r.files, a)
	}

	_, err := actorSystem.Spawn(ctx, "runner", r, actor.WithLongLived())
	if err != nil {
		panic(err)
	}
}

func (r *Runner) Wait() {
	<-r.startWg
	r.workerWg.Wait()
	r.log.Println("End")
}

func (r *Runner) PreStart(ctx *actor.Context) error {
	r.log = ctx.ActorSystem().Logger().(*util.Log)
	return nil
}
func (r *Runner) PostStop(ctx *actor.Context) error {
	if r.multiBar != nil {
		r.multiBar.End()
	}
	return nil
}

func (r *Runner) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		cfg := progressbar.DefaultConfig()
		cfg.ExtraLines = 0
		cfg.ScreenWriter = os.Stdout
		if !r.silent {
			r.multiBar = &progressbar.MultiBar{Config: cfg}
		}

		for i, file := range r.files {
			workerId := fmt.Sprintf("worker-%d", i+1)
			r.log.Println("Importing ", workerId, " ", file)

			if !r.silent {
				cfg.Prefix = filepath.Base(file)
				bar := cfg.NewBar()
				r.multiBar.Add(bar)
				r.bars[file] = bar
			}

			worker := r.Conf.NewWorker(file)
			wpid := ctx.Spawn(workerId, worker, actor.WithLongLived())
			ctx.Watch(wpid)
			ctx.Tell(wpid, &Request{})
			r.workerWg.Add(1)
		}
		if !r.silent {
			r.multiBar.PrefixesAlign()
		}
		close(r.startWg)
	case *goaktpb.Terminated:
		r.log.Println("Worker terminated:", msg.ActorId)
	case *Progress:
		switch State(msg.State) {
		case WorkStateIdle:
			r.log.Println(msg.Src, " starts")
		case WorkStateProgress:
			pct := msg.Progress * 100
			if pct >= 100 {
				pct = 99
			}
			if r.silent {
				r.log.Printf("%s progress: %.2f%%", msg.Src, pct)
			} else {
				r.bars[msg.Src].Progress(pct)
			}
		case WorkStateDone:
			if r.silent {
				r.log.Println(msg.Src, " done.", " success:", msg.Success, ", fail:", msg.Fail)
			} else {
				r.bars[msg.Src].Progress(100)
			}
			r.workerWg.Done()
		case WorkStateError:
			r.log.Println(msg.Src, " ERROR: ", msg.Message)
			r.workerWg.Done()
		}
	default:
		ctx.Unhandled()
	}
}
