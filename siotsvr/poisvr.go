package siotsvr

import (
	"actsvr/feature"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/buntdb"
	"github.com/tochemey/goakt/v3/actor"

	_ "github.com/go-sql-driver/mysql"
)

type PoiServer struct {
	rdb *sql.DB
	gdb *buntdb.DB
}

func NewPoiServer() *PoiServer {
	s := &PoiServer{}
	return s
}

func (s *PoiServer) Featured() {
	feature.Add(s)
}

func (s *PoiServer) Start(ctx context.Context, actorSystem actor.ActorSystem) error {
	if err := s.reload(); err != nil {
		return err
	}
	return nil
}

func (s *PoiServer) Stop(ctx context.Context) error {
	if s.rdb != nil {
		if err := s.rdb.Close(); err != nil {
			return err
		}
	}
	if s.gdb != nil {
		if err := s.gdb.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *PoiServer) reload() error {
	// Open the RDB connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		rdbConfig.user, rdbConfig.pass, rdbConfig.host, rdbConfig.port, rdbConfig.db)
	if rdb, err := sql.Open("mysql", dsn); err != nil {
		return err
	} else {
		s.rdb = rdb
	}
	// Check the RDB connection
	if err := s.rdb.Ping(); err != nil {
		return fmt.Errorf("failed to connect to RDB: %w", err)
	}
	// Open the GeoDB connection
	if gdb, err := buntdb.Open(":memory:"); err != nil {
		return err
	} else {
		s.gdb = gdb
	}
	// Create the GeoDB index
	s.gdb.CreateSpatialIndex("poi", "poi:*:latlon", buntdb.IndexRect)

	// Initialize the GeoDB
	selectAreaCode(s.rdb, func(ac *AreaCode) bool {
		nmKey := fmt.Sprintf("poi:%s:name", ac.AreaCode.String)
		nmValue := ac.AreaNm.String
		latLonKey := fmt.Sprintf("poi:%s:latlon", ac.AreaCode.String)
		latLonValue := fmt.Sprintf("[%f %f]", ac.La, ac.Lo)

		err := s.gdb.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(nmKey, nmValue, nil)
			if err != nil {
				return fmt.Errorf("failed to set area name: %w", err)
			}
			_, _, err = tx.Set(latLonKey, latLonValue, nil)
			if err != nil {
				return fmt.Errorf("failed to set area latlon: %w", err)
			}
			return err
		})
		return err == nil
	})
	return nil
}

func (s *PoiServer) Router(group *gin.RouterGroup) {
	group.POST("/debug", s.handleDebug)
	group.GET("/nearby", s.handleNearby)
}

func (s *PoiServer) handleDebug(c *gin.Context) {
	selectAreaCode(s.rdb, func(ac *AreaCode) bool {
		// data, _ := json.Marshal(ac)
		// fmt.Println(string(data))
		return true
	})
	c.String(http.StatusOK, "POI Server is running")
}

func (s *PoiServer) handleNearby(c *gin.Context) {
	lat := c.Query("la")
	lon := c.Query("lo")
	maxDist := c.DefaultQuery("maxDist", "1000") // Default to 1000 meters
	maxN := c.DefaultQuery("maxN", "10")         // Default to 10 results

	// Convert lat, lon, maxDist, maxN to appropriate types
	latFloat, _ := strconv.ParseFloat(lat, 64)
	lonFloat, _ := strconv.ParseFloat(lon, 64)
	maxDistInt, _ := strconv.Atoi(maxDist)
	maxNInt, _ := strconv.Atoi(maxN)

	results, err := FindNearby(s.gdb, latFloat, lonFloat, maxDistInt, maxNInt)
	if err != nil {
		c.String(http.StatusInternalServerError, "Error querying nearby POIs: %v", err)
		return
	}

	c.JSON(http.StatusOK, results)
}
