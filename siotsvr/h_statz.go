package siotsvr

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/mods/util/metric"
)

var collector *metric.Collector

func Collector(outputFunc metric.OutputFunc) *metric.Collector {
	if collector == nil {
		collector = metric.NewCollector(
			metric.WithSamplingInterval(10*time.Second),
			metric.WithSeries("2h", 60*time.Second, 120),
			metric.WithSeries("2d12h", 30*time.Minute, 120),
			metric.WithPrefix("metrics"),
			metric.WithInputBuffer(50),
		)
		if outputFunc != nil {
			collector.AddOutputFunc(outputFunc)
		}
		collector.AddInputFunc(func(g metric.Gather) {
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
			g.AddMeasurement(m)
		})
	}
	return collector
}

func CollectorHandler() http.Handler {
	dash := metric.NewDashboard(collector)
	dash.PageTitle = "Seoul IoT Server"
	dash.ShowRemains = false
	dash.SetTheme("light")
	return dash
}

type StatRec struct {
	Name string
	Time time.Time
	Val  float64
}

func (s *HttpServer) onProduct(pd metric.Product) {
	var result []StatRec
	switch p := pd.Value.(type) {
	case *metric.CounterValue:
		if p.Samples == 0 {
			return // Skip zero counters
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s:%s", pd.Measure, pd.Field), pd.Time, p.Value}}
	case *metric.GaugeValue:
		if p.Samples == 0 {
			return // Skip zero gauges
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s:%s", pd.Measure, pd.Field), pd.Time, p.Value}}
	case *metric.MeterValue:
		if p.Samples == 0 {
			return // Skip zero meters
		}
		result = []StatRec{
			{fmt.Sprintf("metrics:%s:%s:avg", pd.Measure, pd.Field), pd.Time, p.Sum / float64(p.Samples)},
			{fmt.Sprintf("metrics:%s:%s:max", pd.Measure, pd.Field), pd.Time, p.Max},
			{fmt.Sprintf("metrics:%s:%s:min", pd.Measure, pd.Field), pd.Time, p.Min},
		}
	case *metric.TimerValue:
		if p.Samples == 0 {
			return // Skip zero timers
		}
		result = []StatRec{
			{fmt.Sprintf("metrics:%s:%s:avg", pd.Measure, pd.Field), pd.Time, float64(int64(p.SumDuration) / p.Samples)},
			{fmt.Sprintf("metrics:%s:%s:max", pd.Measure, pd.Field), pd.Time, float64(p.MaxDuration)},
			{fmt.Sprintf("metrics:%s:%s:min", pd.Measure, pd.Field), pd.Time, float64(p.MinDuration)},
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
			result = append(result, StatRec{
				Name: fmt.Sprintf("metrics:%s:%s:p%s", pd.Measure, pd.Field, pct),
				Time: pd.Time,
				Val:  p.Values[i],
			})
		}
	case *metric.OdometerValue:
		if p.Samples == 0 {
			return // Skip zero odometers
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s:%s", pd.Measure, pd.Field), pd.Time, p.Diff()}}
	default:
		defaultLog.Warnf("metrics unknown type: %T", p)
		return
	}
	conn, err := s.openConn(context.TODO())
	if err != nil {
		defaultLog.Error("metrics open conn; ", err)
		return
	}
	defer conn.Close()

	for _, m := range result {
		result := conn.Exec(context.TODO(), "INSERT INTO TAG (name, time, value) VALUES (?, ?, ?)", m.Name, m.Time.UnixNano(), m.Val)
		if err := result.Err(); err != nil {
			defaultLog.Warnf("metrics inserting: %v", err)
			return
		}
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
