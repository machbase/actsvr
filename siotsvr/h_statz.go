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
		collector.AddInputFunc(func(g *metric.Gather) error {
			memStats := runtime.MemStats{}
			runtime.ReadMemStats(&memStats)
			gorutine := runtime.NumGoroutine()
			g.Add("runtime:heap_inuse", float64(memStats.HeapInuse), metric.GaugeType(metric.UnitBytes))
			g.Add("runtime:goroutines", float64(gorutine), metric.GaugeType(metric.UnitShort))
			return nil
		})
	}
	return collector
}

func CollectorHandler() http.Handler {
	lastOnlyFilter := metric.MustCompile([]string{"*#last"})

	dash := metric.NewDashboard(collector)
	dash.PageTitle = "Seoul IoT Server"
	dash.ShowRemains = false
	dash.SetTheme("light")
	dash.AddChart(metric.Chart{Title: "Go Routines", MetricNames: []string{"runtime:goroutines"}, SeriesSelector: lastOnlyFilter})
	dash.AddChart(metric.Chart{Title: "Go Heap In Use", MetricNames: []string{"runtime:heap_inuse"}, SeriesSelector: lastOnlyFilter})
	dash.AddChart(metric.Chart{Title: "HTTP Latency", MetricNames: []string{"http:latency"}})
	dash.AddChart(metric.Chart{Title: "HTTP Status", MetricNames: []string{"http:status_[1-5]xx"}, Type: metric.ChartTypeBarStack})
	dash.AddChart(metric.Chart{Title: "Query Latency", MetricNames: []string{"query:latency"}})
	dash.AddChart(metric.Chart{Title: "Query Error", MetricNames: []string{"query:error"}})
	dash.AddChart(metric.Chart{Title: "Insert Packet", MetricNames: []string{"packet_data:insert_latency"}})
	dash.AddChart(metric.Chart{Title: "Insert Parse", MetricNames: []string{"pars_data:insert_latency"}})
	dash.AddChart(metric.Chart{Title: "Insert Packet RDB", MetricNames: []string{"rdb_packet_data:insert_latency"}})
	dash.AddChart(metric.Chart{Title: "Insert Parse RDB", MetricNames: []string{"rdb_pars_data:insert_latency"}})
	return dash
}

type StatRec struct {
	Name string
	Time time.Time
	Val  float64
}

func (s *HttpServer) onProduct(pd metric.Product) error {
	var result []StatRec
	switch p := pd.Value.(type) {
	case *metric.CounterValue:
		if p.Samples == 0 {
			return nil // Skip zero counters
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s", pd.Name), pd.Time, p.Value}}
	case *metric.GaugeValue:
		if p.Samples == 0 {
			return nil // Skip zero gauges
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s", pd.Name), pd.Time, p.Value}}
	case *metric.MeterValue:
		if p.Samples == 0 {
			return nil // Skip zero meters
		}
		result = []StatRec{
			{fmt.Sprintf("metrics:%s:avg", pd.Name), pd.Time, p.Sum / float64(p.Samples)},
			{fmt.Sprintf("metrics:%s:max", pd.Name), pd.Time, p.Max},
			{fmt.Sprintf("metrics:%s:min", pd.Name), pd.Time, p.Min},
		}
	case *metric.TimerValue:
		if p.Samples == 0 {
			return nil // Skip zero timers
		}
		result = []StatRec{
			{fmt.Sprintf("metrics:%s:avg", pd.Name), pd.Time, float64(int64(p.SumDuration) / p.Samples)},
			{fmt.Sprintf("metrics:%s:max", pd.Name), pd.Time, float64(p.MaxDuration)},
			{fmt.Sprintf("metrics:%s:min", pd.Name), pd.Time, float64(p.MinDuration)},
		}
	case *metric.HistogramValue:
		if p.Samples == 0 {
			return nil // Skip zero histograms
		}
		for i, x := range p.P {
			pct := fmt.Sprintf("%d", int(x*1000))
			if pct[len(pct)-1] == '0' {
				pct = pct[:len(pct)-1]
			}
			result = append(result, StatRec{
				Name: fmt.Sprintf("metrics:%s:p%s", pd.Name, pct),
				Time: pd.Time,
				Val:  p.Values[i],
			})
		}
	case *metric.OdometerValue:
		if p.Samples == 0 {
			return nil // Skip zero odometers
		}
		result = []StatRec{{fmt.Sprintf("metrics:%s", pd.Name), pd.Time, p.Diff()}}
	default:
		defaultLog.Warnf("metrics unknown type: %T", p)
		return nil
	}
	conn, err := s.openConn(context.TODO())
	if err != nil {
		defaultLog.Error("metrics open conn; ", err)
		return err
	}
	defer conn.Close()

	for _, m := range result {
		result := conn.Exec(context.TODO(), "INSERT INTO TAG (name, time, value) VALUES (?, ?, ?)", m.Name, m.Time.UnixNano(), m.Val)
		if err := result.Err(); err != nil {
			defaultLog.Warnf("metrics inserting: %v", err)
			return fmt.Errorf("metrics insert: %w", err)
		}
	}
	return nil
}

func CollectorMiddleware(c *gin.Context) {
	tick := nowFunc()
	c.Next()

	if collector == nil {
		return
	}
	latency := time.Since(tick)
	measure := []metric.Measure{}
	measure = append(measure, metric.Measure{
		Name:  "http:requests",
		Value: 1,
		Type:  metric.CounterType(metric.UnitShort),
	})
	measure = append(measure, metric.Measure{
		Name:  "http:latency",
		Value: float64(latency.Nanoseconds()),
		Type:  metric.HistogramType(metric.UnitDuration),
	})
	measure = append(measure, metric.Measure{
		Name:  fmt.Sprintf("http:status_%dxx", c.Writer.Status()/100),
		Value: 1,
		Type:  metric.CounterType(metric.UnitShort),
	})
	collector.Send(measure...)
}
