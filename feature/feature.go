package feature

import (
	"context"

	"github.com/tochemey/goakt/v3/actor"
)

type Feature func(context.Context, actor.ActorSystem) error

var features []Feature

func AddFeature(f Feature) {
	features = append(features, f)
}

func RunFeatures(ctx context.Context, as actor.ActorSystem) error {
	for _, f := range features {
		if err := f(ctx, as); err != nil {
			return err
		}
	}
	return nil
}
