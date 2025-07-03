package greetings

import (
	"actsvr/feature"
	"context"

	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
)

func init() {
	feature.AddFeature(func(ctx context.Context, as actor.ActorSystem) error {
		_, err := NewRootActor(ctx, as)
		return err
	})
}

type RootActor struct {
}

var _ actor.Actor = (*RootActor)(nil)

const RootActorName = "Root"

func NewRootActor(ctx context.Context, actorSystem actor.ActorSystem) (*actor.PID, error) {
	return actorSystem.Spawn(ctx, RootActorName, &RootActor{},
		actor.WithLongLived(),
		actor.WithSupervisor(
			actor.NewSupervisor(
				actor.WithStrategy(actor.OneForOneStrategy),
				actor.WithAnyErrorDirective(actor.ResumeDirective),
			),
		),
	)
}

func (c *RootActor) PreStart(ctx *actor.Context) error {
	return nil
}

func (c *RootActor) PostStop(ctx *actor.Context) error {
	return nil
}

func (c *RootActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		ga := ctx.Spawn(GreetingActorName, &GreetingActor{}, actor.WithLongLived())
		ctx.Tell(ga, &SayHello{Name: ctx.Self().Name()})
		ctx.Tell(ga, &SayHi{Name: ctx.Self().Name()})
		ctx.Tell(ga, &goaktpb.PoisonPill{})
	case *SayStop:
		log := ctx.ActorSystem().Logger()
		log.Infof("Received message: --------> SayStop: %s", msg.Name)
	}
}
