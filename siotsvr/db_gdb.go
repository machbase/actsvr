package siotsvr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/tidwall/buntdb"
)

func FindAreaName(gdb *buntdb.DB, areaCode string) (string, error) {
	var areaName string
	err := gdb.View(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("poi:%s:name", areaCode)
		value, err := tx.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get area name for %s: %w", areaCode, err)
		}
		areaName = value
		return nil
	})
	if err != nil {
		return "", err
	}
	return areaName, nil
}

func FindAreaLatLon(gdb *buntdb.DB, areaCode string) (float64, float64, error) {
	var areaCoord string
	err := gdb.View(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("poi:%s:latlon", areaCode)
		value, err := tx.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get area name for %s: %w", areaCode, err)
		}
		areaCoord = value
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return ParseLatLon(areaCoord)
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
			if len(results) < maxN {
				areaCode := strings.TrimPrefix(key, "poi:")
				areaCode = strings.TrimSuffix(areaCode, ":latlon")
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
				nm, err := FindAreaName(gdb, areaCode)
				if err != nil {
					return false // Failed to get area name
				}
				result := NearbyResult{
					AreaCode: areaCode,
					AreaNm:   nm,
					Lat:      areaLat,
					Lon:      areaLon,
					Dist:     dist,
				}
				results = append(results, result)
				return true // Continue iterating until we have n results
			}
			return false // Stop iterating once we have enough results
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query GeoDB: %w", err)
	}
	return results, nil
}

type NearbyResult struct {
	AreaCode string  `json:"area_code"`
	AreaNm   string  `json:"area_nm"`
	Lat      float64 `json:"la"`
	Lon      float64 `json:"lo"`
	Dist     int     `json:"dist"`
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
