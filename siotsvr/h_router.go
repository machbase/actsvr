package siotsvr

import (
	"embed"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tochemey/goakt/v3/log"
)

//go:embed static
var htmlFS embed.FS

func (s *HttpServer) Router() *gin.Engine {
	if s.router == nil {
		s.router = s.buildRouter()
	}
	return s.router
}

func (s *HttpServer) buildRouter() *gin.Engine {
	if s.log.LogLevel() == log.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/static/") })
	r.GET("/static/", gin.WrapF(http.FileServerFS(htmlFS).ServeHTTP))
	r.GET("/db/write/RecptnPacketData", s.writeRecptnPacketData)
	r.POST("/db/write/RecptnPacketData", s.writeRecptnPacketData)
	r.GET("/db/write/PacketParsData", s.writePacketParsData)
	r.POST("/db/write/PacketParsData", s.writePacketParsData)
	r.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "404 Not Found")
	})
	return r
}
