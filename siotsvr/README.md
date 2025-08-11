

### POI nearby search by coord

It returns Nth near-by points from the given coordinate.

```http
GET /poi/nearby?la=37.5990998&lo=126.9861493&n=3
```

**params**

- `la` Lat
- `lo` Lon
- `n` max result count

**response**

```json
[
    {"area_code":"11110", "area_nm":"종로구", "dist":0, "la":37.5991, "lo":126.986149},
    {"area_code":"11290", "area_nm":"성북구", "dist":3381, "la":37.606991, "lo":127.023218},
    {"area_code":"11140", "area_nm":"중구", "dist":4630, "la":37.557945, "lo":126.99419}
]
```

### POI nearby search by areaCode

It returns Nth near-by points from the given `area_code`.

```http
GET /poi/nearby?area_code=11110&n=3
```

**params**

- `area_code` Area Code
- `n` max result count

**response**

```json
[
    {"area_code":"11110", "area_nm":"종로구", "dist":0, "la":37.5991, "lo":126.986149},
    {"area_code":"11290", "area_nm":"성북구", "dist":3381, "la":37.606991, "lo":127.023218},
    {"area_code":"11140", "area_nm":"중구", "dist":4630, "la":37.557945, "lo":126.99419}
]
```
