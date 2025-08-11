package siotsvr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"encoding/json"

	"github.com/tidwall/buntdb"
)

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
