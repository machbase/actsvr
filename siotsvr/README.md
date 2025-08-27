
## start flags

**start-siotsvr.sh**

```sh
#!/bin/bash
exec /data/app/machbase/siotsvr \
	-pid /data/app/machbase/siotsvr.pid \
	-http-host 0.0.0.0 \
	-http-port 5680 \
	-db-host 127.0.0.1 \
	-db-port 5656 \
	-db-user ${MACHBASE_USER} \
	-db-pass ${MACHBASE_PASS} \
	-rdb-host 127.0.0.1 \
	-rdb-port 3306 \
	-rdb-user ${MARIADB_USER} \
	-rdb-pass ${MARISDB_PASS} \
	-rdb-db ${MARIADB_DB} \
	-last-dir /data/app/machbase/data \
	-last-limit 1000 \
	-log-max-backups 10 \
	-log-verbose 2 \
	-log-filename /data/app/machbase/log/siotsvr.log
```

- `-pid` pid를 기록하는 파일 경로
- `-http-host`, `-http-port` API 서버의 Listening IP와 Port
- `-http-datadir` API 서버의 temp directory (default: /tmp)
- `-db-host`, `-db-port`, `-db-user`, `-db-pass` machbase 연결 정보
- `-rdb-host`, `-rdb-port`, `-rdb-user`, `-rdb-pass`, `-rdb-db` maria db 연결 정보
- `-last-dir` 마지막 arrival time을 기록하는 파일을 저장할 디렉터리 경로
- `-last-limit` arrival time 기준으로 조회할 때 query limit (0일 경우 무제한, 디폴트 1000)
- `-log-max-backups` 로그파일 rotation limit
- `-log-verbose` 0: 디폴트 (warning, error 만 기록)  1: info 이상 기록, 2: 모든 로그 기록
- `-log-filename` 로그 경로와 파일명

## API

- [POI API](./docs/api_poi.md)

- [Reload Cached Reference Data](./docs/api_reload.md)

- [Adjust Log Verbose](./docs/api_log.md)

