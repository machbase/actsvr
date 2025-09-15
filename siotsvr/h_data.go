package siotsvr

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/mods/util/metric"
)

func (s *HttpServer) handleData(c *gin.Context) {
	tick := time.Now()
	var requestErr string
	cancel := false
	var nrow int = 0
	var orgId int64 = -1
	var orgName string = ""
	var tsn int64 = -1
	defer func() {
		// Stat
		if orgId > 0 && tsn > 0 && nrow > 0 {
			s.statCh <- &StatDatum{
				orgId: orgId,
				tsn:   tsn,
				nrow:  nrow,
				ts:    tick,
				url:   c.Request.URL.Path + "?" + c.Request.URL.RawQuery,
			}
		}
		// Request의 패킷 정보를 로깅
		req := c.Request.URL.Path
		if c.Request.URL.RawQuery != "" {
			req += "?" + c.Request.URL.RawQuery
		}
		reply := fmt.Sprintf("%q org:%d tsn:%d nrow:%d", orgName, orgId, tsn, nrow)
		if requestErr != "" {
			reply = requestErr
		}
		if cancel {
			reply += " (canceled)"
		}
		latency := time.Since(tick)
		msgs := []any{
			c.Writer.Status(),
			" ", latency,
			" ", reply,
		}
		msgs = append(msgs, " ", req)
		if defaultLog.DebugEnabled() { // only with verbose log level
			if sqlText := c.GetString("SQL"); sqlText != "" {
				msgs = append(msgs, " ", sqlText)
			}
		}
		if requestErr == "" {
			defaultLog.Info(msgs...)
		} else {
			defaultLog.Warn(msgs...)
		}

		if collector != nil {
			measure := []metric.Measure{}
			if requestErr == "" {
				measure = append(measure, metric.Measure{
					Name:  "query:latency",
					Value: float64(latency.Nanoseconds()),
					Type:  metric.HistogramType(metric.UnitDuration),
				})
			} else {
				measure = append(measure, metric.Measure{
					Name:  "query:error",
					Value: 1,
					Type:  metric.CounterType(metric.UnitShort),
				})
			}
			collector.Send(measure...)
		}
	}()
	// Path params
	tsnStr := c.Param("tsn")
	dataNoStr := c.Param("data_no")
	// Query params
	certkey := c.Query("certKey")
	modelSerial := c.Query("modelSerial")
	areaCode := c.Query("areaCode")
	start := c.Query("start")
	end := c.Query("end")
	rqsrc := c.Query("RQSRC")
	isParsStr := c.Query("ISPARS")

	_ = rqsrc // Unused variable, but kept for compatibility

	var isPars = isParsStr == "Y" || isParsStr == "y"

	var now = nowFunc().In(DefaultTZ)
	if no, err := strconv.ParseInt(tsnStr, 10, 64); err != nil {
		requestErr = "invalid_tsn"
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
		return
	} else {
		tsn = no
	}
	if key, err := getOrgKey(certkey); err != nil {
		requestErr = "wrong_certkey"
		c.JSON(http.StatusForbidden, ApiErrorInvalidCertkey)
		return
	} else {
		if key.BeginValidDe.After(now) || key.EndValidDe.Before(now) {
			requestErr = "certkey_expired"
			c.JSON(http.StatusForbidden, ApiErrorExpiredCertkey)
			return
		}
		orgId = key.CertkeySeq
		orgName = key.OrgName
		if key.OrgCName != "" {
			orgName = orgName + "(" + key.OrgCName + ")"
		}
	}

	var dataNo int
	if no, err := strconv.Atoi(dataNoStr); err != nil || no < 1 || no > 3 {
		requestErr = "invalid_data_no"
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
		return
	} else {
		dataNo = no
	}
	var err error
	var startTime time.Time
	var endTime time.Time
	if start != "" {
		startTime, err = time.ParseInLocation("20060102150405", start, DefaultTZ)
		if err != nil {
			requestErr = "invalid_start_time"
			c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
			return
		}
	}
	if end != "" {
		endTime, err = time.ParseInLocation("20060102150405", end, DefaultTZ)
		if err != nil {
			requestErr = "invalid_end_time"
			c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
			return
		}
	}
	if startTime.IsZero() && endTime.IsZero() {
		// start와 end가 존재하지 않는 경우 현재시간 이전 30분으로 설정된다.
		endTime = time.Now()
		startTime = endTime.Add(-30 * time.Minute)
	} else if startTime.IsZero() && !endTime.IsZero() {
		if modelSerial == "" {
			// end만 존재할 경우 start는 end 시간의 60분 전으로 설정된다.
			startTime = endTime.Add(-60 * time.Minute)
		} else {
			// end만 존재할 경우 start는 end 시간의 30분 전으로 설정된다.
			startTime = endTime.Add(-30 * time.Minute)
		}
	} else if !startTime.IsZero() && endTime.IsZero() {
		// start만 존재할 경우 end는 start 시간의 60분 후로 설정된다.
		endTime = startTime.Add(60 * time.Minute)
	}

	conn, err := s.openConn(c)
	if err != nil {
		defaultLog.Errorf("Failed to open database connection: %v", err)
		c.JSON(http.StatusInternalServerError, ApiErrorServer)
		return
	}
	defer conn.Close()

	if isPars {
		nrow, cancel = handleParsData(c, conn, tsn, dataNo, startTime, endTime, modelSerial, areaCode)
	} else {
		nrow, cancel = handleRawData(c, conn, tsn, dataNo, startTime, endTime, modelSerial, areaCode)
	}
}

func handleParsData(c *gin.Context, conn api.Conn, tsn int64, dataNo int, startTime time.Time, endTime time.Time, modelSerial string, areaCode string) (nrow int, cancel bool) {
	searchDataNo := 1 // always use data_no = 1 for model data
	definition := getPacketDefinition(tsn, searchDataNo)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for tsn: %d and data_no: %d", tsn, dataNo)
		c.JSON(http.StatusNotFound, ApiErrorServer)
		return
	}
	dqmInfo := getModelDqmInfo(tsn)
	if dqmInfo != nil && !dqmInfo.Public {
		defaultLog.Errorf("Packet is not public for tsn: %d and data_no: %d", tsn, dataNo)
		c.JSON(http.StatusForbidden, ApiErrorNonPublic)
		return
	}
	sb := &strings.Builder{}
	args := []any{}
	var arrivalTime time.Time

	sb.WriteString(`SELECT _ARRIVAL_TIME, PACKET_PARS_SEQ, MODL_SERIAL, REGIST_DT, AREA_CODE`)
	for i := range definition.Fields {
		sb.WriteString(fmt.Sprintf(", COLUMN%d", i))
	}
	sb.WriteString(` FROM `)
	sb.WriteString(tableName(`TB_PACKET_PARS_DATA`))
	if dataNo == 1 {
		sb.WriteString(` WHERE REGIST_DT >= ? AND REGIST_DT <= ?`)
		args = append(args, startTime.UnixNano(), endTime.UnixNano())
		if modelSerial != "" {
			sb.WriteString(` AND MODL_SERIAL = ?`)
			args = append(args, modelSerial)
		} else {
			sb.WriteString(` AND TRNSMIT_SERVER_NO = ?`)
			args = append(args, tsn)
			sb.WriteString(` AND DATA_NO = ?`)
			args = append(args, dataNo)
		}
		if areaCode != "" {
			sb.WriteString(` AND AREA_CODE = ?`)
			args = append(args, areaCode)
		}
	} else {
		parsDataArrivalTime.Lock()
		defer parsDataArrivalTime.Unlock()

		sb.WriteString(` WHERE _ARRIVAL_TIME > ?`)
		args = append(args, parsDataArrivalTime.Time)
		sb.WriteString(` AND DATA_NO = ?`)
		args = append(args, dataNo)
		sb.WriteString(` ORDER BY _ARRIVAL_TIME`)
		if arrivalQueryLimit > 0 {
			sb.WriteString(` LIMIT ?`)
			args = append(args, arrivalQueryLimit)
		}

		defer func() {
			if parsDataArrivalTime.Time.After(arrivalTime) {
				return
			}
			if disableUpdateArrivalTime {
				// 시험을 위해 time을 update 하지 않음
			} else {
				parsDataArrivalTime.Time = arrivalTime
				parsDataArrivalTime.Save()
			}
			if log := DefaultLog(); log != nil && log.InfoEnabled() {
				log.Infof("last arrival time for pars data: %s",
					parsDataArrivalTime.Time.In(DefaultTZ).Format("2006-01-02 15:04:05.000000000"))
			}
		}()
	}

	sqlText := sb.String()

	// set SQL on the context for logging
	c.Set("SQL", fmt.Sprintf("%s; %v", sqlText, args))

	// execute the SQL
	rows, err := conn.Query(c, sqlText, args...)
	if err != nil {
		defaultLog.Errorf("Failed to query database: %v", err)
		c.JSON(http.StatusInternalServerError, ApiErrorServer)
		return
	}
	defer rows.Close()

	dataNoOut := dataNo
	switch dataNoOut {
	case 3:
		dataNoOut = 2
	case 2:
		dataNoOut = 1
	}
	c.Header("Content-Type", "application/json")
	c.Writer.WriteString(`{`)
	fmt.Fprintf(c.Writer, `"dataNo":"%d",`, dataNoOut)
	fmt.Fprintf(c.Writer, `"datasetNo":"%d",`, tsn)
	fmt.Fprintf(c.Writer, `"resultCode":"SUCC-000","resultMsg":"조회 완료",`)
	fmt.Fprintf(c.Writer, `"startDateTime":"%s",`, startTime.In(DefaultTZ).Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"endDateTime":"%s",`, endTime.In(DefaultTZ).Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"resultdata":[`)

	go func() {
		<-c.Done()
		cancel = true
	}()

	for rows.Next() && !cancel {
		var seq int64
		var modelSerial string
		var date time.Time
		var areaCode string
		values := make([]string, len(definition.Fields))

		buff := []any{&arrivalTime, &seq, &modelSerial, &date, &areaCode}
		for i := range values {
			buff = append(buff, &values[i])
		}
		if err := rows.Scan(buff...); err != nil {
			defaultLog.Errorf("Failed to scan row: %v", err)
			c.JSON(http.StatusInternalServerError, ApiErrorServer)
			return
		}
		if nrow > 0 {
			c.Writer.WriteString(",")
		}
		fmt.Fprintf(c.Writer, `{"seq":%d,"serial":"%s","date":"%s","areaCode":"%s","pars":[`,
			seq, modelSerial, date.In(DefaultTZ).Format("2006-01-02 15:04:05"), areaCode)
		cntField := 0
		for i, value := range values {
			if cntField > 0 {
				c.Writer.WriteString(",")
			}
			if fd := definition.Fields[i]; fd.Public {
				if dqmInfo != nil && dqmInfo.Masking {
					value = MaskingStrValue
				}
				fmt.Fprintf(c.Writer, `%q`, value)
				cntField++
			}
		}
		c.Writer.WriteString(`]}`)
		nrow++
	}
	c.Writer.WriteString(`]}`)
	return
}

func handleRawData(c *gin.Context, conn api.Conn, tsn int64, dataNo int, startTime time.Time, endTime time.Time, modelSerial string, areaCode string) (nrow int, cancel bool) {
	sb := &strings.Builder{}
	args := []any{}
	var arrivalTime time.Time

	sb.WriteString(`SELECT _ARRIVAL_TIME, PACKET_SEQ, MODL_SERIAL, REGIST_DT, AREA_CODE, PACKET`)
	sb.WriteString(` FROM `)
	sb.WriteString(tableName(`TB_RECPTN_PACKET_DATA`))
	if dataNo == 1 {
		sb.WriteString(` WHERE REGIST_DT >= ? AND REGIST_DT <= ?`)
		sb.WriteString(` AND TRNSMIT_SERVER_NO = ?`)
		sb.WriteString(` AND DATA_NO = ?`)
		args = append(args,
			startTime.UnixNano(),
			endTime.UnixNano(),
			tsn,
			dataNo,
		)
		if modelSerial != "" {
			sb.WriteString(` AND MODL_SERIAL = ?`)
			args = append(args, modelSerial)
		}
		if areaCode != "" {
			sb.WriteString(` AND AREA_CODE = ?`)
			args = append(args, areaCode)
		}
	} else {
		packetDataArrivalTime.Lock()
		defer packetDataArrivalTime.Unlock()
		sb.WriteString(` WHERE _ARRIVAL_TIME > ?`)
		args = append(args, packetDataArrivalTime.Time)
		sb.WriteString(` AND DATA_NO = ?`)
		args = append(args, dataNo)
		sb.WriteString(` ORDER BY _ARRIVAL_TIME`)
		if arrivalQueryLimit > 0 {
			sb.WriteString(` LIMIT ?`)
			args = append(args, arrivalQueryLimit)
		}

		defer func() {
			if packetDataArrivalTime.Time.After(arrivalTime) {
				return
			}
			if disableUpdateArrivalTime {
				// 시험을 위해 time을 update 하지 않음
			} else {
				packetDataArrivalTime.Time = arrivalTime
				packetDataArrivalTime.Save()
			}
			if log := DefaultLog(); log != nil && log.InfoEnabled() {
				log.Infof("last arrival time for packet data: %s",
					packetDataArrivalTime.Time.In(DefaultTZ).Format("2006-01-02 15:04:05.000000000"))
			}
		}()
	}

	sqlText := sb.String()

	// set SQL on the context for logging
	c.Set("SQL", fmt.Sprintf("%s; %v", sqlText, args))

	// execute the SQL
	rows, err := conn.Query(c, sqlText, args...)
	if err != nil {
		defaultLog.Errorf("Failed to query database: %v", err)
		c.JSON(http.StatusInternalServerError, ApiErrorServer)
		return
	}
	defer rows.Close()

	dataNoOut := dataNo
	switch dataNoOut {
	case 3:
		dataNoOut = 2
	case 2:
		dataNoOut = 1
	}

	c.Header("Content-Type", "application/json")
	c.Writer.WriteString(`{`)
	fmt.Fprintf(c.Writer, `"dataNo":"%d",`, dataNoOut)
	fmt.Fprintf(c.Writer, `"datasetNo":"%d",`, tsn)
	fmt.Fprintf(c.Writer, `"resultCode":"SUCC-000","resultMsg":"조회 완료",`)
	fmt.Fprintf(c.Writer, `"startDateTime":"%s",`, startTime.In(DefaultTZ).Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"endDateTime":"%s",`, endTime.In(DefaultTZ).Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"resultdata":[`)

	if ctx := c.Request.Context(); ctx != nil {
		go func(ctx context.Context) {
			<-ctx.Done()
			cancel = true
		}(ctx)
	}
	for rows.Next() && !cancel {
		var seq int64
		var modelSerial string
		var date time.Time
		var areaCode string
		var packet string

		if err := rows.Scan(&arrivalTime, &seq, &modelSerial, &date, &areaCode, &packet); err != nil {
			defaultLog.Errorf("Failed to scan row: %v", err)
			c.JSON(http.StatusInternalServerError, ApiErrorServer)
			return
		}
		if nrow > 0 {
			c.Writer.WriteString(",")
		}
		fmt.Fprintf(c.Writer, `{"seq":%d,"serial":"%s","date":"%s","areaCode":"%s","packet":"%s"}`,
			seq, modelSerial, date.In(DefaultTZ).Format("2006-01-02 15:04:05"), areaCode, packet)
		nrow++
	}
	c.Writer.WriteString(`]}`)
	return
}
