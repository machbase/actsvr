package siotsvr

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	_ "github.com/go-sql-driver/mysql"
)

func (s *HttpServer) handlePoiNearby(c *gin.Context) {
	cachePoiMutex.RLock()
	defer func() {
		cachePoiMutex.RUnlock()
		if e := recover(); e != nil {
			c.String(http.StatusInternalServerError, "Internal server error: %v", e)
			return
		}
	}()

	wantHtml := c.DefaultQuery("html", "false") == "true"
	areaCode := c.Query("area_code")
	lat := c.Query("la")
	lon := c.Query("lo")
	maxN := c.DefaultQuery("n", "10") // Default to 10 results
	maxNInt, _ := strconv.Atoi(maxN)
	modlSerial := c.Query("modl_serial")
	trnsmitServerNo := c.Query("trnsmit_server_no")
	dataNo := c.Query("data_no")

	var latFloat, lonFloat float64
	if modlSerial != "" && trnsmitServerNo != "" && dataNo != "" {
		trnsmitServerNoInt, err := strconv.Atoi(trnsmitServerNo)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid transmit server number: %v", err)
			return
		}
		dataNoInt, err := strconv.Atoi(dataNo)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid data number: %v", err)
			return
		}
		latFloat, lonFloat, err = POIFindLatLon(cachePoi, fmt.Sprintf("%s_%d_%d", modlSerial, trnsmitServerNoInt, dataNoInt))
		if err != nil {
			c.String(http.StatusInternalServerError, "Error finding model lat/lon: %v", err)
			return
		}
	} else if areaCode != "" {
		var err error
		// If areaCode is provided, find nearby points based on area code
		latFloat, lonFloat, err = POIFindLatLon(cachePoi, areaCode)
		if err != nil {
			c.String(http.StatusInternalServerError, "Error finding area lat/lon: %v", err)
			return
		}
		if latFloat == 0 && lonFloat == 0 {
			c.String(http.StatusNotFound, "Area code not found")
			return
		}
	} else {
		// Convert lat, lon, maxN to appropriate types
		latFloat, _ = strconv.ParseFloat(lat, 64)
		lonFloat, _ = strconv.ParseFloat(lon, 64)
	}

	results, err := FindNearby(cachePoi, latFloat, lonFloat, maxNInt)
	if err != nil {
		c.String(http.StatusInternalServerError, "Error querying nearby POIs: %v", err)
		return
	}

	if wantHtml {
		renderPoiHTML(c, results)
	} else {
		c.JSON(http.StatusOK, results)
	}
}

func renderPoiHTML(c *gin.Context, results []NearbyResult) {
	contents := []string{HeaderTemplate, BaseTemplate, HtmlTemplate}
	tpl := template.New("geomap").Funcs(template.FuncMap{
		"safeJS": func(s interface{}) template.JS {
			return template.JS(fmt.Sprint(s))
		},
	})
	tpl = template.Must(tpl.Parse(contents[0]))
	for _, cont := range contents[1:] {
		tpl = template.Must(tpl.Parse(cont))
	}
	data := map[string]interface{}{
		"PageTitle":     "POI Map",
		"TileGrayscale": 100,
		"GeomapID":      "geomap",
		"Width":         "100%",
		"Height":        "100%",
		"Markers":       []Marker{},
		"Center":        [2]float64{},
	}
	for i, res := range results {
		lat := res["la"].(float64)
		lon := res["lo"].(float64)
		data["Markers"] = append(data["Markers"].([]Marker), Marker{Lat: lat, Lon: lon, Label: res.String()})
		if i == 0 {
			data["Center"] = [2]float64{lat, lon}
		}
	}

	tpl.ExecuteTemplate(c.Writer, "geomap", data)
}

type Marker struct {
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Label string  `json:"label"`
}

var HeaderTemplate = `
{{ define "header" }}
<head>
    <meta charset="UTF-8">
    <title>{{ .PageTitle }}</title>
	<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
	<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<style>
    .geomap_container { width:100%; height:100%; display: flex;justify-content: center;align-items: center;}
    .geomap_item {margin: auto;}
    .leaflet-tile-pane{ -webkit-filter: grayscale({{ .TileGrayscale }}%); filter: grayscale({{ .TileGrayscale }}%);}
</style>
</head>
{{ end }}
`

var BaseTemplate = `
{{- define "base" }}
<div class="geomap_container">
    <div class="geomap_item" id="{{ .GeomapID }}" style="width:{{ .Width }};height:{{ .Height }};"></div>
</div>
{{- range .JSCodeAssets }}
<script src="{{ . }}" type="text/javascript" charset="UTF-8"></script>
{{- end }}
{{ end }}
`

var HtmlTemplate = `{{- define "geomap" }}<!DOCTYPE html>
<html>
    {{- template "header" . }}
<body style="width:100vw; height:100vh">
    {{- template "base" . }}
<script>
	var map = L.map("{{ .GeomapID }}", {crs: L.CRS.EPSG3857, attributionControl:false});
	map.setView([{{ index .Center 0 }}, {{ index .Center 1 }}], 13);
	L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);
	{{- range .Markers }}
	L.marker([{{ .Lat }}, {{ .Lon }}]).addTo(map).bindPopup('{{ .Label }}');
	{{- end }}
</script>
</body>
</html>
{{ end }}
`
