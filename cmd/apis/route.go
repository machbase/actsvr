package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) Router() *gin.Engine {
	r := gin.New()
	r.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Welcome to the HTTP server!")
	})
	return r
}
