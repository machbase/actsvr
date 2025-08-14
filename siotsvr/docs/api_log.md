
## Adjust runtime log verbose level 

운영 중에 로그 레벨을 변경.

```http
GET http://127.0.0.1:5680/db/admin/log?verbose={{0|1|2}}
```

- `verbose=0` no verbose log, only warning and error logs.
- `verbose=1` INFO level logs
- `verbose=2` DEBUG and INFO logs

