package greetings

import (
	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

type GreetingActor struct {
	self   *actor.PID
	parent *actor.PID
}

var _ actor.Actor = (*GreetingActor)(nil)

const GreetingActorName = "Greetings"

func (c *GreetingActor) PreStart(ctx *actor.Context) error {
	log := ctx.ActorSystem().Logger()
	log.Info(ctx.ActorName(), " --------> pre-start")
	return nil
}

func (c *GreetingActor) PostStop(ctx *actor.Context) error {
	log := ctx.ActorSystem().Logger()
	log.Info(ctx.ActorName(), " --------> post-stop")
	if c.parent != nil {
		c.self.Tell(ctx.Context(), c.parent, &SayStop{Name: ctx.ActorName()})
	}
	return nil
}

func (c *GreetingActor) Receive(ctx *actor.ReceiveContext) {
	log := ctx.ActorSystem().Logger()
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		log.Infof("Received message: --------> PostStart")
		c.self = ctx.Self()
		c.parent = c.self.Parent()
	case *SayHello:
		log.Infof("Received message: --------> SayHello: %s", msg.Name)
	case *SayHi:
		log.Infof("Received message: --------> SayHi: %s", msg.Name)
	}
}
