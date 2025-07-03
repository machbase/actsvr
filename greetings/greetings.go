package greetings

import (
	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
	"github.com/tochemey/goakt/v3/log"
)

type GreetingActor struct {
	self   *actor.PID
	parent *actor.PID
	log    log.Logger
}

var _ actor.Actor = (*GreetingActor)(nil)

const GreetingActorName = "Greetings"

func (c *GreetingActor) PreStart(ctx *actor.Context) error {
	c.log = ctx.ActorSystem().Logger()
	c.log.Info(ctx.ActorName(), " --------> pre-start")
	return nil
}

func (c *GreetingActor) PostStop(ctx *actor.Context) error {
	c.log.Info(ctx.ActorName(), " --------> post-stop")
	if c.parent != nil {
		c.self.Tell(ctx.Context(), c.parent, &Bye{Name: ctx.ActorName()})
	}
	return nil
}

func (c *GreetingActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		c.log.Infof("Received message: --------> PostStart")
		c.self = ctx.Self()
		c.parent = c.self.Parent()
	case *SayHello:
		c.log.Infof("Received message: --------> SayHello: %s", msg.Name)
	case *SayHi:
		c.log.Infof("Received message: --------> SayHi: %s", msg.Name)
	}
}
