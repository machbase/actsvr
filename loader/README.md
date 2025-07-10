## Loader

`loader` is importing tool from CSV file(s) to a machbase table.

### Usage

```sh
loader [flags...] <file1> <file2> ...
```

**ex)**

```sh
loader -db-host 127.0.0.1 -db-port 5656 \
       -db-user sys -db-pass manager \
       -db-table target_table \
       ./data/file1.csv ./data/file2.csv
```

### Flags

**Destination Table**

  - `-db-host string`
        Database host (default "127.0.0.1")
  - `-db-port int`
        Database port (default 5656)
  - `-db-user string`
        Database user (default "sys")
  - `-db-pass string`
        Database password (default "manager")
  - `-db-table string`
        Database table name

**CSV format**
  - `-skip-header`
        Skip the first line of the CSV file (header)
  - `-timeformat string`
        Time format for the CSV file, e.g., 'ns', 'us', 'ms', 's', '2006-01-02 15:04:05' (default "ns")
  - `-tz string`
        Time zone for the CSV file, e.g., 'UTC', 'Local', 'Asia/Seoul' (default "Local")

**Log**

  - `-log-filename string`
        the log file name (default "-")
  - `-log-timeformat string`
        the time format to use in the log file (default "2006-01-02 15:04:05.000000 Z0700 MST")
  - `-log-utc`
        whether to use local time in the log file
  - `-log-verbose int`
        0: no debug, 1: info, 2: debug
  - `-log-append`
        whether to append to the log file or overwrite it
  - `-log-compress`
        whether to compress the log file
  - `-log-max-age int`
        the maximum age of the log file in days (default 28)
  - `-log-max-backups int`
        the maximum number of log file backups (default 3)
  - `-log-max-size int`
        the maximum size of the log file in megabytes (default 100)

**Progress bar**

  - `-silent`
        If true, suppresses progress output
