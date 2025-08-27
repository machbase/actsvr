package siotsvr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/OutOfBedlam/metric"
	"github.com/gin-gonic/gin"
)

var collector *metric.Collector

func Collector() *metric.Collector {
	if collector == nil {
		collector = metric.NewCollector(
			metric.WithCollectInterval(1*time.Second),
			metric.WithSeriesListener("30m", 10*time.Second, 180, onProduct),
			metric.WithExpvarPrefix("metrics"),
			metric.WithReceiverSize(100),
		)
		collector.AddInputFunc(func() (metric.Measurement, error) {
			m := metric.Measurement{Name: "runtime"}

			memStats := runtime.MemStats{}
			runtime.ReadMemStats(&memStats)
			gorutine := runtime.NumGoroutine()
			m.Fields = []metric.Field{
				{
					Name:  "heap_inuse",
					Value: float64(memStats.HeapInuse),
					Unit:  metric.UnitBytes,
					Type:  metric.FieldTypeGauge,
				},
				{
					Name:  "goroutines",
					Value: float64(gorutine),
					Unit:  metric.UnitShort,
					Type:  metric.FieldTypeMeter,
				},
			}
			return m, nil
		})
	}
	return collector
}

func onProduct(pd metric.ProducedData) {
	var result []any
	switch p := pd.Value.(type) {
	case *metric.CounterProduct:
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
	case *metric.GaugeProduct:
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
	case *metric.MeterProduct:
		if p.Samples == 0 {
			return // Skip zero meters
		}
		result = []any{
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:max", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Max,
			},
			map[string]any{
				"NAME":  fmt.Sprintf("metrics:%s:%s:avg", pd.Measure, pd.Field),
				"TIME":  pd.Time.UnixNano(),
				"VALUE": p.Sum / float64(p.Samples),
			},
		}
	case *metric.HistogramProduct:
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
	measure.AddField(metric.Field{Name: "requests", Value: 1, Unit: metric.UnitShort, Type: metric.FieldTypeCounter})
	measure.AddField(metric.Field{Name: "latency", Value: float64(latency.Nanoseconds()), Unit: metric.UnitDuration, Type: metric.FieldTypeHistogram(100, 0.5, 0.9, 0.99)})
	statusCat := ""
	switch sc := c.Writer.Status(); {
	case sc >= 100 && sc < 200:
		statusCat = "status_1xx"
	case sc >= 200 && sc < 300:
		statusCat = "status_2xx"
	case sc >= 300 && sc < 400:
		statusCat = "status_3xx"
	case sc >= 400 && sc < 500:
		statusCat = "status_4xx"
	case sc >= 500:
		statusCat = "status_5xx"
	}
	measure.AddField(metric.Field{Name: statusCat, Value: 1, Unit: metric.UnitShort, Type: metric.FieldTypeCounter})
	collector.SendEvent(measure)
}
