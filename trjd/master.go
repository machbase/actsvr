package trjd

import (
	"actsvr/feature"
	"actsvr/util"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

type Config struct {
	Host      string
	Port      int
	MaxWorker int
}

type Master struct {
	Conf Config

	pid         *actor.PID // Master actor PID
	workers     []string
	workersLock sync.RWMutex
	wrokerSeq   int64
	log         *util.Log // Logger
}

var _ feature.Feature = (*Master)(nil)
var _ actor.Actor = (*Master)(nil)

const MasterName = "trjd-master"

func NewMaster(cfg Config) *Master {
	return &Master{Conf: cfg}
}

func (c *Master) Featured() {
	feature.Add(c)
}

// feature.Feature interface implementation
func (c *Master) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	pid, err := actorSystem.Spawn(ctx, MasterName, c,
		actor.WithLongLived(),
		actor.WithSupervisor(
			actor.NewSupervisor(
				actor.WithStrategy(actor.OneForOneStrategy),
				actor.WithAnyErrorDirective(actor.ResumeDirective),
			),
		),
	)
	c.pid = pid
	c.log = actorSystem.Logger().(*util.Log)
	return err
}

// feature.Feature interface implementation
func (c *Master) Stop(ctx context.Context) error {
	return nil
}

func (c *Master) PreStart(ctx *actor.Context) error {
	return nil
}

func (c *Master) PostStop(ctx *actor.Context) error {
	return nil
}

func (c *Master) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *ImportRequest:
		worker := c.createWorker(ctx)
		ctx.Watch(worker)
		ctx.Forward(worker)
	case *goaktpb.Terminated:
		c.terminatedWorker(msg)
	default:
		ctx.Unhandled()
	}
}

func (c *Master) terminatedWorker(msg *goaktpb.Terminated) {
	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	c.log.Println("Worker terminated:", msg.ActorId)
	// If a worker terminates, we can remove it from the list
	for i, w := range c.workers {
		if w == msg.ActorId {
			c.workers = append(c.workers[:i], c.workers[i+1:]...)
			break
		}
	}
}

func (c *Master) createWorker(ctx *actor.ReceiveContext) *actor.PID {
	c.workersLock.Lock()
	defer c.workersLock.Unlock()

	seq := atomic.AddInt64(&c.wrokerSeq, 1)
	name := fmt.Sprintf("worker-%d", seq)
	c.workers = append(c.workers, name)

	pid := ctx.Spawn(name, &Worker{})
	return pid
}
