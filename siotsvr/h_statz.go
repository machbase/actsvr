package siotsvr

import (
	"expvar"
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/OutOfBedlam/metric"
	metric_ps "github.com/OutOfBedlam/metrical/input/ps"
	metric_runtime "github.com/OutOfBedlam/metrical/input/runtime"
	"github.com/OutOfBedlam/metrical/output/svg"
	"github.com/gin-gonic/gin"
)

var collector *metric.Collector

func Collector(dataDir string) *metric.Collector {
	if collector == nil {
		collector = metric.NewCollector(1*time.Second,
			metric.WithSeries("30m", 10*time.Second, 180),
			metric.WithSeries("10h", 5*time.Minute, 120),
			metric.WithSeries("8d", 1*time.Hour, 192),
			metric.WithExpvarPrefix("metrics"),
			metric.WithReceiverSize(100),
			metric.WithStorage(metric.NewFileStorage(filepath.Join(dataDir, "metrics"))),
		)
		collector.AddInputFunc(metric_ps.Collect)
		collector.AddInputFunc(metric_runtime.Collect)
	}
	return collector
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

func (s *HttpServer) handleAdminStatz(c *gin.Context) {
	q := c.Request.URL.Query()
	name := q.Get("n")
	idx := 0
	if name != "" {
		if str := q.Get("i"); str != "" {
			fmt.Sscanf(str, "%d", &idx)
		}
	}
	if str := q.Get("r"); str != "" {
		if refresh, err := fmt.Sscanf(str, "%d", &idx); err == nil {
			if refresh > 0 {
				c.Writer.Header().Set("Refresh", fmt.Sprintf("%d", refresh))
			}
		}
	}
	var data = Data{}
	if name == "" {
		data.MetricNames = []string{
			"metrics:ps:cpu_percent",
			"metrics:ps:mem_percent",
			"metrics:runtime:goroutines",
			"metrics:runtime:heap_inuse",
			"metrics:query:latency",
			"metrics:query:error",
			"metrics:packet_data:insert_latency",
			"metrics:packet_data:insert_error",
			"metrics:packet_data:parse_error",
			"metrics:packet_err:insert_latency",
			"metrics:packet_err:insert_error",
			"metrics:pars_data:insert_latency",
			"metrics:pars_data:insert_error",
			"metrics:rdb_packet_data:insert_latency",
			"metrics:rdb_packet_data:insert_error",
			"metrics:rdb_pars_data:insert_latency",
			"metrics:rdb_pars_data:insert_error",
			"metrics:http:requests",
			"metrics:http:latency",
			"metrics:http:status_1xx",
			"metrics:http:status_2xx",
			"metrics:http:status_3xx",
			"metrics:http:status_4xx",
			"metrics:http:status_5xx",
		}
	} else {
		data.MetricNames = []string{name}
		data.Snapshot = getSnapshot(name, idx)
		if data.Snapshot == nil {
			c.String(http.StatusNotFound, "Metric not found")
			return
		}
	}
	c.HTML(http.StatusOK, "statz", data)
}

func getSnapshot(name string, idx int) *metric.Snapshot {
	if g := expvar.Get(name); g != nil {
		mts := g.(metric.MetricTimeSeries)
		if len(mts) > 0 {
			return mts[idx].Snapshot()
		}
	}
	return nil
}

type Data struct {
	MetricNames []string
	Snapshot    *metric.Snapshot
}

var tmplFuncMap = template.FuncMap{
	"snapshotAll":        SnapshotAll,
	"snapshotField":      SnapshotField,
	"productValueString": ProductValueString,
	"productKind":        ProductKind,
	"miniGraph":          MiniGraph,
	"formatTime":         func(t time.Time) string { return t.Format(time.TimeOnly) },
}

var tmplStatz = template.Must(template.New("statz").Funcs(tmplFuncMap).
	Parse(`<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<meta http-equiv="cache-control" content="no-cache, no-store, must-revalidate">
	<meta http-equiv="cache-control" content="max-age=0">
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="expires" content="0">
	<title>Metrics</title>
	<style>
		body { font-family: Arial, sans-serif; }
		table { width: 100%; border-collapse: collapse; }
		th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
		th { background-color: #f2f2f2; }
		tr:hover { background-color: #f1f1f1; }
		.graphRow {
			display: flex;
			justify-content: flex-start;
			flex-direction: row;
			flex-wrap: wrap;
		}
		.graph {
			flex: 0;
			margin-left: 10px;
			margin-right: 20px;
		}
	</style>
</head>
<body>
<h1>Metrics</h1>
{{ if .Snapshot }}
	{{ template "doDetail" . }}
{{ else }}
	{{ template "doMiniGraph" . }}
{{end}}
</body>
</html>

{{ define "doMiniGraph" }}
 	{{range $n, $name := .MetricNames}}
		<h2>{{$name}}</h2>
		<div class="graphRow">
		{{ range $idx, $ss := snapshotAll $name }}
		 <div class="graph">
			<a href="?n={{ $name }}&i={{ $idx }}">{{ $ss | miniGraph }}</a>
		</div>
		{{ end }}
		 </div>
	{{end}}
{{ end }}

{{ define "doDetail" }}
 	{{ $ss := .Snapshot }}
	{{ $field := snapshotField $ss }}
	<h2>{{ $field.Name }} ({{ $ss | productKind }})</h2>
	<table>
		<tr>
			<th>Time</th>
			<th>Value</th>
			<th>JSON</th>
		</tr>
		{{ range $idx, $val := $ss.Values }}
		<tr>
		<td>{{ index $ss.Times $idx | formatTime }}</td>
		<td>{{ productValueString $val $field.Unit }}</td>
		<td>{{ $val }}</td>
		</tr>
		{{end}}
	</table>
{{ end }}
`))

func MiniGraph(ss *metric.Snapshot) template.HTML {
	canvas := svg.CanvasWithSnapshot(ss)
	buff := &strings.Builder{}
	canvas.Export(buff)
	return template.HTML(buff.String())
}

func SnapshotAll(name string) []*metric.Snapshot {
	ret := make([]*metric.Snapshot, 0)
	if g := expvar.Get(name); g != nil {
		mts := g.(metric.MetricTimeSeries)
		for _, ts := range mts {
			snapshot := ts.Snapshot()
			if snapshot != nil {
				ret = append(ret, snapshot)
			}
		}
	}
	return ret
}

func SnapshotField(ss *metric.Snapshot) metric.FieldInfo {
	f, _ := ss.Field()
	return f
}

func ProductValueString(p metric.Product, unit metric.Unit) string {
	if p == nil {
		return "null"
	}
	return unit.Format(ProductValue(p), 2)
}

func ProductValue(p metric.Product) float64 {
	switch v := p.(type) {
	case *metric.CounterProduct:
		return v.Value
	case *metric.GaugeProduct:
		return v.Value
	case *metric.MeterProduct:
		if v.Count > 0 {
			return v.Sum / float64(v.Count)
		}
		return 0
	case *metric.HistogramProduct:
		if len(v.Values) > 0 {
			return v.Values[len(v.Values)/2]
		}
		return 0
	default:
		return 0
	}
}

func ProductKind(ss *metric.Snapshot) string {
	p := ss.Values[0]
	switch p.(type) {
	case *metric.CounterProduct:
		return "Counter"
	case *metric.GaugeProduct:
		return "Gauge"
	case *metric.MeterProduct:
		return "Meter"
	case *metric.HistogramProduct:
		return "Histogram"
	default:
		return "Unknown"
	}
}
