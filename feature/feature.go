package feature

import (
	"context"

	"github.com/tochemey/goakt/v3/actor"
)

type Feature interface {
	Start(context.Context, actor.ActorSystem) error
	Stop(context.Context) error
}

type FeatureFunc func(context.Context, actor.ActorSystem) error

var features []Feature

func Add(f Feature) {
	features = append(features, f)
}

func AddFunc(f FeatureFunc) {
	features = append(features, &featureFuncWrapper{f: f})
}

type featureFuncWrapper struct {
	f FeatureFunc
}

func (w *featureFuncWrapper) Start(ctx context.Context, as actor.ActorSystem) error {
	return w.f(ctx, as)
}
func (w *featureFuncWrapper) Stop(ctx context.Context) error {
	return nil
}

func StartFeatures(ctx context.Context, as actor.ActorSystem) error {
	for _, f := range features {
		if err := f.Start(ctx, as); err != nil {
			return err
		}
	}
	return nil
}

func StopFeatures(ctx context.Context) error {
	for _, f := range features {
		if err := f.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}
