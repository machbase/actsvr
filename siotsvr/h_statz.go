package siotsvr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/mods/util/metric"
)

var collector *metric.Collector

func Collector() *metric.Collector {
	if collector == nil {
		collector = metric.NewCollector(
			metric.WithInterval(1*time.Second),
			metric.WithSeries("30m", 10*time.Second, 180),
			metric.WithPrefix("metrics"),
			metric.WithInputBuffer(100),
		)
		collector.AddOutputFunc(onProduct)
		collector.AddInputFunc(func() (metric.Measurement, error) {
			m := metric.Measurement{Name: "runtime"}

			memStats := runtime.MemStats{}
			runtime.ReadMemStats(&memStats)
			gorutine := runtime.NumGoroutine()
			m.Fields = []metric.Field{
				{
					Name:  "heap_inuse",
					Value: float64(memStats.HeapInuse),
					Type:  metric.GaugeType(metric.UnitBytes),
				},
				{
					Name:  "goroutines",
					Value: float64(gorutine),
					Type:  metric.MeterType(metric.UnitShort),
				},
			}
			return m, nil
		})
	}
	return collector
}

func onProduct(pd metric.Product) {
	var result []any
	switch p := pd.Value.(type) {
	case *metric.CounterValue:
		if p.Samples == 0 {
			return // Skip zero counters
		}
		result = []any{
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Value,
			},
		}
	case *metric.GaugeValue:
		if p.Samples == 0 {
			return // Skip zero gauges
		}
		result = []any{
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Value,
			},
		}
	case *metric.MeterValue:
		if p.Samples == 0 {
			return // Skip zero meters
		}
		result = []any{
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:avg", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Sum / float64(p.Samples),
			},
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:max", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Max,
			},
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:min", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Min,
			},
		}
	case *metric.TimerValue:
		if p.Samples == 0 {
			return // Skip zero timers
		}
		result = []any{
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:avg", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": int64(p.SumDuration) / p.Samples,
			},
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:max", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.MaxDuration,
			},
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:min", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.MinDuration,
			},
		}
	case *metric.HistogramValue:
		if p.Samples == 0 {
			return // Skip zero meters
		}
		for i, x := range p.P {
			pct := fmt.Sprintf("%d", int(x*1000))
			if pct[len(pct)-1] == '0' {
				pct = pct[:len(pct)-1]
			}
			result = append(result, map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:p%s", pd.Measure, pd.Field, pct),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Values[i],
			})
		}
	default:
		defaultLog.Warnf("metrics unknown type: %T", p)
		return
	}
	out := &bytes.Buffer{}
	for _, m := range result {
		b, err := json.Marshal(m)
		if err != nil {
			defaultLog.Warnf("metrics marshaling: %v", err)
			return
		}
		out.Write(b)
		out.Write([]byte("\n"))
	}
	out.Write([]byte("\n"))

	rsp, err := http.DefaultClient.Post(
		fmt.Sprintf("http://%s:5654/db/write/TAG", machConfig.dbHost),
		"application/x-ndjson", out)
	if err != nil {
		defaultLog.Warnf("metrics sending: %v", err)
		return
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(rsp.Body)
		defaultLog.Warnf("metrics writing: %s", msg)
		return
	}
}

func CollectorMiddleware(c *gin.Context) {
	tick := nowFunc()
	c.Next()

	if collector == nil {
		return
	}
	latency := time.Since(tick)
	measure := metric.Measurement{Name: "http"}
	measure.AddField(metric.Field{
		Name:  "requests",
		Value: 1,
		Type:  metric.CounterType(metric.UnitShort),
	})
	measure.AddField(metric.Field{
		Name:  "latency",
		Value: float64(latency.Nanoseconds()),
		Type:  metric.HistogramType(metric.UnitDuration),
	})
	measure.AddField(metric.Field{
		Name:  fmt.Sprintf("status_%dxx", c.Writer.Status()/100),
		Value: 1,
		Type:  metric.CounterType(metric.UnitShort),
	})
	collector.Send(measure)
}
