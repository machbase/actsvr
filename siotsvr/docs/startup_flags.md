
## General

| Flag           | Default       | Description          |
|:---------------|:--------------|:---------------------|
| `-pid <path>`  |               | PID 저장 파일 경로      |
| `-dev`         |               | NTB_* 사용 (시험 목적)  |

## Machbase

| Flag                | Default       | Description     |
|:--------------------|:--------------|:----------------|
| `-db-host <ip>`     | `127.0.0.1`   | IP Address      |
| `-db-port <port>`   | `5656`        | Port            |
| `-db-user <user>`   | `sys`         | database user   |
| `-db-pass <pass>`   | `manager`     | password        |


## Arrival time store

| Flag                | Default       | Description     |
|:--------------------|:--------------|:----------------|
| `-last-dir <dir>`   | `.`           | 마지막 arrival time 저장 디렉터리 |
| `-last-limit <num>` | `1000`        | 한 번에 전송할 데이터의 레코드 최대 수 |
| `-last-no-update`   |               | 마지막 arrival time을 업데이트하지 않도록 함 (시험 목적) |

* `-last-dir` 에 지정된 디렉터리에 아래와 같은 파일이 생성됨, 파일의 시간 지정 포맷은 `2025-10-02 08:27:52.000000000`.

- last_packet.txt
- last_pars.txt

## RDB

| Flag                | Default       | Description        |
|:--------------------|:--------------|:-------------------|
| `-rdb-host <ip>`    |               | mariadb IP Address |
| `-rdb-port <port>`  | `3306`        | mariadb port       |
| `-rdb-user <user>`  | `iotdb`       | user               |
| `-rdb-pass <pass>`  | `iotmanager`  | password           |
| `-rdb-db <db>`      | `iotdb`       | database name      |


## HTTP Server

| Flag                      | Default       | Description     |
|:--------------------------|:--------------|:----------------|
| `-http-host <ip>`         | `0.0.0.0`     | listening addr  |
| `-http-port <port>`       | `8888`        | listening port  |
| `-http-keepalive <num>`   | `60`          | keep-alive period (seconds), 0: no keep-alive |
| `-http-datadir <dir>`     | `/tmp`        | *Not Used* (ignore) |

* Http Listener : NoDelay = true, SO_LINGER = 0

## Machbase->RDB Replication

| Flag                    | Default       | Description     |
|:------------------------|:--------------|:----------------|
| `-replica-rows <num>`   | `200`         | num. of record: replication batch size  |

## Log

| Flag                    | Default       | Description     |
|:------------------------|:--------------|:----------------|
| `-log-filename <path>`  | `-`           | 로그 파일 경로, `-`: stdout |
| `-log-max-size <num>`   | `100`         | 로그 파일 최대 크기(MB) |
| `-log-max-backups <num>`| `3`           | 로그 백업 파일 최대 수  |
| `-log-max-age <num>`    | `28`          | 로그 백업 파일 유지 최대 일 (day) |
| `-log-compress`         |               | 로그 백업시 압축 유무 |
| `-log-utc`              |               | 로그 시간 UTC 기준 (디폴트: 로컬 타임존 = Asia/Seoul) |
| `-log-verbose`          | `0`           | `0`: none, `1`: info, `2`: debug |

### 운영 중에 로그 레벨을 변경.

```http
GET http://127.0.0.1:5680/db/admin/log?verbose={0|1|2}
```

- `verbose=0` no verbose log, only warning and error logs.
- `verbose=1` INFO level logs
- `verbose=2` DEBUG and INFO logs

