package trjd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v3/log"
	"github.com/tochemey/goakt/v3/testkit"
)

func TestWorker(t *testing.T) {
	t.Run("worker_for_import", func(t *testing.T) {
		// create a test context
		ctx := context.TODO()
		// create a test kit
		testkit := testkit.New(ctx, t, testkit.WithLogging(log.WarningLevel))

		// create the test probe
		probe := testkit.NewProbe(ctx)

		// create the actor
		p, _ := testkit.ActorSystem().Spawn(ctx, "worker", &Worker{})
		probe.Watch(p)

		workId := NewWorkId().String()
		src := "./test_data/CN7_10LINES.CSV"
		probe.Send("worker", &ImportRequest{
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
		probe.ExpectMessageWithin(30*time.Second, idle)

		done := &ImportProgress{
			WorkId:   workId,
			Src:      src,
			State:    int32(WorkStateDone),
			Message:  "",
			Progress: 1.0,
			Success:  8,
			Fail:     0,
		}
		probe.ExpectMessageWithin(30*time.Second, done)

		err := p.Shutdown(ctx)
		require.NoError(t, err)

		probe.ExpectTerminated(p.Name())
		probe.ExpectNoMessage()

		// shutdown the test context
		probe.Stop()
		testkit.Shutdown(ctx)
	})
}
