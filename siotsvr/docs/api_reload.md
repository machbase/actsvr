
## Reload Reference Data

Reload Cache data from RDBMS

### TB_CERTKEY

```http
GET http://127.0.0.1:5680/db/admin/reload?target=certkey
```

### TB_MODL_PACKET_MASTR and TB_MODL_PACKET_DETAIL

```http
GET http://127.0.0.1:5680/db/admin/reload?target=model
```

### TB_MODL_INSTALL_INFO and TB_AREA_CODE

```http
GET http://127.0.0.1:5680/db/admin/reload?target=model_areacode
```

### PACKET_SEQ

It re-read max value from machbase table.

``sql
SELECT MAX(PACKET_PARS_SEQ) FROM TB_RECPTN_PACKET_DATA
```

```http
GET http://127.0.0.1:5680/db/admin/reload?target=packet_seq
```

### PACKET_PARSE_SEQ

It re-read max value from machbase table.

``sql
SELECT MAX(PACKET_PARS_SEQ) FROM TB_PACKET_PARS_DATA
```

```http
GET http://127.0.0.1:5680/db/admin/reload?target=packet_parse_seq
```
