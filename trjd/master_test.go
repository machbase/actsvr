package trjd

import (
	"actsvr/feature"
	"context"
	"testing"
	"time"

	"github.com/tochemey/goakt/v3/log"
	"github.com/tochemey/goakt/v3/testkit"
)

func TestMaster(t *testing.T) {
	t.Run("import_data", func(t *testing.T) {
		// create a test context
		ctx := context.TODO()
		// create a test kit
		testkit := testkit.New(ctx, t, testkit.WithLogging(log.WarningLevel))

		// create the actor
		m := NewMaster(Config{})

		feature.Add(m)
		feature.StartFeatures(ctx, testkit.ActorSystem())
		defer feature.StopFeatures(ctx)

		// create the test probe
		probe := testkit.NewProbe(ctx)

		workId := NewWorkId().String()
		src := "./test_data/tmp/data1/CN7_2023-04-06_15-57-39.CSV"

		probe.Send(MasterName, &ImportRequest{
			WorkId:   workId,
			Src:      src,
			DstHost:  "127.0.0.1",
			DstPort:  int32(testServer.MachPort()),
			DstUser:  "sys",
			DstPass:  "manager",
			DstTable: "trip",
		})

		idle := &ImportProgress{
			WorkId:   workId,
			Src:      src,
			State:    int32(WorkStateIdle),
			Message:  "",
			Progress: 0.0,
		}
		probe.ExpectMessageWithin(60*time.Second, idle)

		done := &ImportProgress{
			WorkId:   workId,
			Src:      src,
			State:    int32(WorkStateDone),
			Message:  "",
			Progress: 1.0,
			Success:  460075,
			Fail:     0,
		}
		probe.ExpectMessageWithin(60*time.Second, done)

		// shutdown the test context
		probe.Stop()
		testkit.Shutdown(ctx)
	})
}
