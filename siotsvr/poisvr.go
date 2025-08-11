package siotsvr

import (
	"actsvr/util"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/buntdb"

	_ "github.com/go-sql-driver/mysql"
)

type PoiServer struct {
	log      *util.Log
	rdb      *sql.DB
	gdb      *buntdb.DB
	gdbMutex sync.RWMutex
}

func NewPoiServer() *PoiServer {
	return &PoiServer{}
}

func (s *PoiServer) Start(ctx context.Context) error {
	s.log = DefaultLog()
	s.log.Info("Starting PoiServer...")
	if gdb, err := s.reload(); err != nil {
		return err
	} else {
		s.gdbMutex.Lock()
		s.gdb = gdb
		s.gdbMutex.Unlock()
	}
	return nil
}

func (s *PoiServer) Stop(ctx context.Context) error {
	if s.rdb != nil {
		if err := s.rdb.Close(); err != nil {
			return err
		}
	}
	s.gdbMutex.Lock()
	defer s.gdbMutex.Unlock()
	if s.gdb != nil {
		if err := s.gdb.Close(); err != nil {
			return err
		}
	}
	s.log.Info("PoiServer stopped.")
	return nil
}

func (s *PoiServer) reload() (*buntdb.DB, error) {
	// Open the RDB connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		rdbConfig.user, rdbConfig.pass, rdbConfig.host, rdbConfig.port, rdbConfig.db)
	if rdb, err := sql.Open("mysql", dsn); err != nil {
		return nil, err
	} else {
		s.rdb = rdb
	}
	// Check the RDB connection
	if err := s.rdb.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to RDB: %w", err)
	}
	// Open the GeoDB connection
	gdb, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, err
	}
	// Create the GeoDB index
	gdb.CreateSpatialIndex("poi", "poi:*:latlon", buntdb.IndexRect)

	// Load from AreaCode
	err = selectAreaCode(s.rdb, func(ac *AreaCode) bool {
		nmKey := fmt.Sprintf("poi:%s:data", ac.AreaCode.String)
		nmMap := map[string]any{
			"area_code": ac.AreaCode.String,
			"area_nm":   ac.AreaNm.String,
		}
		nmRaw, _ := json.Marshal(nmMap)
		nmValue := string(nmRaw)
		latLonKey := fmt.Sprintf("poi:%s:latlon", ac.AreaCode.String)
		latLonValue := fmt.Sprintf("[%f %f]", ac.La, ac.Lo)

		err := gdb.Update(func(tx *buntdb.Tx) error {
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
	if err != nil {
		return nil, fmt.Errorf("failed to initialize GeoDB from AreaCode: %w", err)
	}
	// Load from ModelInstallInfo
	err = selectModlInstlInfo(s.rdb, func(mi *ModelInstallInfo, merr error) bool {
		if merr != nil {
			s.log.Warnf("failed to load ModelInstallInfo: %v", merr)
			return true // Continue processing other records
		}
		key := fmt.Sprintf("%s_%d_%d", mi.ModlSerial, mi.TrnsmitServerNo, mi.DataNo)
		nmKey := fmt.Sprintf("poi:%s:data", key)
		nmMap := map[string]any{
			"modl_serial":       mi.ModlSerial,
			"trnsmit_server_no": mi.TrnsmitServerNo,
			"data_no":           mi.DataNo,
			"buld_nm":           nullString(mi.BuldNm),
			"instl_floor":       nullInt64(mi.InstlFloor),
			"instl_ho_no":       nullInt64(mi.InstlHoNo),
			"adres":             nullString(mi.Adres),
			"adres_detail":      nullString(mi.AdresDetail),
			"la":                mi.La,
			"lo":                mi.Lo,
		}
		nmRaw, _ := json.Marshal(nmMap)
		nmValue := string(nmRaw)
		latLonKey := fmt.Sprintf("poi:%s:latlon", key)
		latLonValue := fmt.Sprintf("[%f %f]", mi.La, mi.Lo)
		err := gdb.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(nmKey, nmValue, nil)
			if err != nil {
				return fmt.Errorf("failed to set model name: %w", err)
			}
			_, _, err = tx.Set(latLonKey, latLonValue, nil)
			if err != nil {
				return fmt.Errorf("failed to set model latlon: %w", err)
			}
			return err
		})
		return err == nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize GeoDB from ModelInstallInfo: %w", err)
	}
	s.log.Info("PoiServer GeoDB initialized successfully.")
	return gdb, nil
}

func (s *PoiServer) Router(group *gin.RouterGroup) {
	group.GET("/nearby", s.handleNearby)
	group.POST("/reload", s.handleReload)
}

func (s *PoiServer) handleReload(c *gin.Context) {
	if gdb, err := s.reload(); err != nil {
		c.String(http.StatusInternalServerError, "Error reloading PoiServer: %v", err)
	} else {
		s.gdbMutex.Lock()
		defer s.gdbMutex.Unlock()
		if s.gdb != nil {
			if err := s.gdb.Close(); err != nil {
				defaultLog.Warnf("Error closing old GeoDB: %v", err)
			}
		}
		s.gdb = gdb
		c.String(http.StatusOK, "PoiServer reloaded successfully")
	}
}

func (s *PoiServer) handleNearby(c *gin.Context) {
	s.gdbMutex.RLock()
	defer s.gdbMutex.RUnlock()

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
		latFloat, lonFloat, err = POIFindLatLon(s.gdb, fmt.Sprintf("%s_%d_%d", modlSerial, trnsmitServerNoInt, dataNoInt))
		if err != nil {
			c.String(http.StatusInternalServerError, "Error finding model lat/lon: %v", err)
			return
		}
	} else if areaCode != "" {
		var err error
		// If areaCode is provided, find nearby points based on area code
		latFloat, lonFloat, err = POIFindLatLon(s.gdb, areaCode)
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

	results, err := FindNearby(s.gdb, latFloat, lonFloat, maxNInt)
	if err != nil {
		c.String(http.StatusInternalServerError, "Error querying nearby POIs: %v", err)
		return
	}

	c.JSON(http.StatusOK, results)
}
