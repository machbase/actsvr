package siotsvr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"encoding/json"

	"github.com/tidwall/buntdb"
)

var cachePoi *buntdb.DB
var cachePoiMutex sync.RWMutex

func reloadPoiData() error {
	// Open the RDB connection
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return err
	}
	defer rdb.Close()

	// Check the RDB connection
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to connect to RDB: %w", err)
	}
	// Open the GeoDB connection
	gdb, err := buntdb.Open(":memory:")
	if err != nil {
		return err
	}
	// Create the GeoDB index
	gdb.CreateSpatialIndex("poi", "poi:*:latlon", buntdb.IndexRect)

	// Load from AreaCode
	err = SelectAreaCode(rdb, func(ac *AreaCode) bool {
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
		return fmt.Errorf("failed to initialize GeoDB from AreaCode: %w", err)
	}
	poiCount := 0
	// Load from ModelInstallInfo
	err = SelectModelInstallInfo(rdb, func(mi *ModelInstallInfo, merr error) bool {
		if merr != nil {
			defaultLog.Warnf("failed to load ModelInstallInfo: %v", merr)
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
			poiCount++
			return err
		})
		return err == nil
	})
	if err != nil {
		return fmt.Errorf("failed to initialize GeoDB from ModelInstallInfo: %w", err)
	}
	defaultLog.Infof("Loaded %d poi", poiCount)
	cachePoiMutex.Lock()
	oldGdb := cachePoi
	cachePoi = gdb
	cachePoiMutex.Unlock()
	if oldGdb != nil {
		if err := oldGdb.Close(); err != nil {
			defaultLog.Warnf("Error closing old POI GeoDB: %v", err)
		}
	}
	return nil
}

func POIFindData(gdb *buntdb.DB, poiKey string) (string, error) {
	var data string
	err := gdb.View(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("poi:%s:data", poiKey)
		value, err := tx.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get poi data for %q %w", key, err)
		}
		data = value
		return nil
	})
	if err != nil {
		return "", err
	}
	return data, nil
}

func POIFindLatLon(gdb *buntdb.DB, areaCode string) (float64, float64, error) {
	var coord string
	err := gdb.View(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("poi:%s:latlon", areaCode)
		value, err := tx.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get area name for %s: %w", areaCode, err)
		}
		coord = value
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return ParseLatLon(coord)
}

func ParseLatLon(latLon string) (float64, float64, error) {
	// Remove brackets and split by space to get lat and lon
	trimmedValue := strings.Trim(latLon, "[]")
	latLonParts := strings.Split(trimmedValue, " ")
	if len(latLonParts) != 2 {
		return 0, 0, fmt.Errorf("invalid latlon format: %s", latLon)
	}
	lat, err := strconv.ParseFloat(latLonParts[0], 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid latitude: %w", err)
	}
	lon, err := strconv.ParseFloat(latLonParts[1], 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid longitude: %w", err)
	}
	return lat, lon, nil
}

// FindNearby retrieves nearby points of interest from the GeoDB based on latitude and longitude.
// It returns a slice of AreaCode representing the nearby points.
func FindNearby(gdb *buntdb.DB, lat, lon float64, maxN int) ([]NearbyResult, error) {
	var results []NearbyResult
	point := fmt.Sprintf("[%f %f]", lat, lon)
	err := gdb.View(func(tx *buntdb.Tx) error {
		// Query the GeoDB for nearby points of interest
		tx.Nearby("poi", point, func(key, value string, dist0 float64) bool {
			if len(results) >= maxN {
				return false // Stop iterating once we have enough results
			}
			poiKey := strings.TrimPrefix(key, "poi:")
			poiKey = strings.TrimSuffix(poiKey, ":latlon")
			// Parse the latlon value to get latitude and longitude

			// Remove brackets and split by space to get lat and lon
			trimmedValue := strings.Trim(value, "[]")
			latLonParts := strings.Split(trimmedValue, " ")
			if len(latLonParts) != 2 {
				return false // Invalid latlon format
			}
			areaLat, err := strconv.ParseFloat(latLonParts[0], 64)
			if err != nil {
				return false // Invalid latitude
			}
			areaLon, err := strconv.ParseFloat(latLonParts[1], 64)
			if err != nil {
				return false // Invalid longitude
			}
			dist := haversine(lat, lon, areaLat, areaLon)
			rawData, err := POIFindData(gdb, poiKey)
			if err != nil {
				return false // Failed to get poi data
			}
			data := map[string]any{}
			if err := json.Unmarshal([]byte(rawData), &data); err != nil {
				return false // Failed to unmarshal poi data
			}
			data["la"] = areaLat
			data["lo"] = areaLon
			data["dist"] = dist
			results = append(results, data)
			return true // Continue iterating until we have n results
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query GeoDB: %w", err)
	}
	return results, nil
}

type NearbyResult map[string]any

func (r NearbyResult) String() string {
	jsonData, _ := json.Marshal(r)
	return string(jsonData)
}

func haversine(lat1, lon1, lat2, lon2 float64) int {
	const R = 6371000 // Radius of the Earth in meters
	lat1Rad := lat1 * (3.141592653589793 / 180)
	lon1Rad := lon1 * (3.141592653589793 / 180)
	lat2Rad := lat2 * (3.141592653589793 / 180)
	lon2Rad := lon2 * (3.141592653589793 / 180)

	dLat := lat2Rad - lat1Rad
	dLon := lon2Rad - lon1Rad

	a := (math.Sin(dLat/2) * math.Sin(dLat/2)) + math.Cos(lat1Rad)*math.Cos(lat2Rad)*(math.Sin(dLon/2)*math.Sin(dLon/2))
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return int(R * c) // Distance in meters
}
