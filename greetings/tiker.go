package greetings

import (
	"time"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

type TickerActor struct {
	self   *actor.PID
	parent *actor.PID
}

var _ actor.Actor = (*TickerActor)(nil)

const TickerActorName = "Ticker"

func (c *TickerActor) PreStart(ctx *actor.Context) error {
	return nil
}

func (c *TickerActor) PostStop(ctx *actor.Context) error {
	if c.parent != nil {
		c.self.Tell(ctx.Context(), c.parent, &Bye{Name: ctx.ActorName()})
	}
	return nil
}

func (c *TickerActor) Receive(ctx *actor.ReceiveContext) {
	log := ctx.ActorSystem().Logger()
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		c.self = ctx.Self()
		c.parent = c.self.Parent()
	case *Tick:
		log.Infof("Received message: --------> Tick: %s", time.Unix(0, msg.Tick))
	}
}
