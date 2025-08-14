package siotsvr

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
)

func (s *HttpServer) handleData(c *gin.Context) {
	tick := time.Now()
	nrow := 0
	defer func() {
		// Request의 패킷 정보를 로깅
		req := c.Request.URL.Path
		if c.Request.URL.RawQuery != "" {
			req += "?" + c.Request.URL.RawQuery
		}
		defaultLog.Info(c.Writer.Status(), " ", time.Since(tick), " ", nrow, " ", req)
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

	var tsn int64
	var now = nowFunc().In(DefaultLocation)
	if no, err := strconv.ParseInt(tsnStr, 10, 64); err != nil {
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
	} else {
		tsn = no
	}
	if key, err := getCertKey(certkey); err != nil {
		c.JSON(http.StatusForbidden, ApiErrorInvalidCertkey)
		return
	} else {
		if !key.TrnsmitServerNo.Valid {
			c.JSON(http.StatusForbidden, ApiErrorInvalidCertkey)
			return
		}
		if key.TrnsmitServerNo.Int64 != tsn {
			c.JSON(http.StatusForbidden, ApiErrorWrongCertkey)
			return
		}
		if key.BeginValidDe.After(now) || key.EndValidDe.Before(now) {
			c.JSON(http.StatusForbidden, ApiErrorExpiredCertkey)
			return
		}
	}

	var dataNo int
	if no, err := strconv.Atoi(dataNoStr); err != nil {
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
		return
	} else {
		dataNo = no
	}
	var err error
	var startTime time.Time
	var endTime time.Time
	if start != "" {
		startTime, err = time.Parse("20060102150405", start)
		if err != nil {
			c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
			return
		}
	}
	if end != "" {
		endTime, err = time.Parse("20060102150405", end)
		if err != nil {
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
		nrow = handleParsData(c, conn, tsn, dataNo, startTime, endTime, modelSerial, areaCode)
	} else {
		nrow = handleRawData(c, conn, tsn, dataNo, startTime, endTime, modelSerial, areaCode)
	}
}

func handleParsData(c *gin.Context, conn api.Conn, tsn int64, dataNo int, startTime time.Time, endTime time.Time, modelSerial string, areaCode string) (nrow int) {
	definition := getPacketDefinition(tsn, dataNo)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for tsn: %d and data_no: %d", tsn, dataNo)
		c.JSON(http.StatusNotFound, ApiErrorServer)
		return
	}
	sb := &strings.Builder{}
	sb.WriteString(`select
			PACKET_PARS_SEQ,
			MODL_SERIAL,
			REGIST_DT,
			AREA_CODE`)

	for i := range definition.Fields {
		sb.WriteString(fmt.Sprintf(", COLUMN%d", i))
	}
	sb.WriteString(`
		from
			TB_PACKET_PARS_DATA 
		where
			REGIST_DT >= ? and REGIST_DT <= ?
	`)
	args := []any{
		startTime.UnixNano(),
		endTime.UnixNano(),
	}
	if modelSerial != "" {
		sb.WriteString(` and MODL_SERIAL = ?`)
		args = append(args, modelSerial)
	} else {
		sb.WriteString(` and TRNSMIT_SERVER_NO = ?`)
		args = append(args, tsn)
		sb.WriteString(` and DATA_NO = ?`)
		args = append(args, dataNo)
	}
	if areaCode != "" {
		sb.WriteString(` and AREA_CODE = ?`)
		args = append(args, areaCode)
	}

	rows, err := conn.Query(c, sb.String(), args...)
	if err != nil {
		defaultLog.Errorf("Failed to query database: %v", err)
		c.JSON(http.StatusInternalServerError, ApiErrorServer)
		return
	}
	defer rows.Close()

	c.Header("Content-Type", "application/json")
	c.Writer.WriteString(`{`)
	fmt.Fprintf(c.Writer, `"dataNo":"%d",`, dataNo)
	fmt.Fprintf(c.Writer, `"datasetNo":"%d",`, tsn)
	fmt.Fprintf(c.Writer, `"resultCode":"SUCC-000","resultMsg":"전송 완료",`)
	fmt.Fprintf(c.Writer, `"startDateTime":"%s",`, startTime.Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"endDateTime":"%s",`, endTime.Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"resultdata":[`)
	for rows.Next() {
		var seq int64
		var modelSerial string
		var date time.Time
		var areaCode string
		values := make([]string, len(definition.Fields))

		buff := []any{&seq, &modelSerial, &date, &areaCode}
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
			seq, modelSerial, date.Format("2006-01-02 15:04:05"), areaCode)
		for i, value := range values {
			if i > 0 {
				c.Writer.WriteString(",")
			}
			fmt.Fprintf(c.Writer, `%q`, value)
		}
		c.Writer.WriteString(`]}`)
		nrow++
	}
	c.Writer.WriteString(`]}`)
	return
}

func handleRawData(c *gin.Context, conn api.Conn, tsn int64, dataNo int, startTime time.Time, endTime time.Time, modelSerial string, areaCode string) (nrow int) {
	sb := &strings.Builder{}
	sb.WriteString(`select
			PACKET_SEQ,
			MODL_SERIAL,
			REGIST_DT,
			AREA_CODE,
			PACKET
		from
			TB_RECPTN_PACKET_DATA 
		where
			REGIST_DT >= ? and REGIST_DT <= ?
		and TRNSMIT_SERVER_NO = ?
		and DATA_NO = ? 
	`)
	args := []any{
		startTime.UnixNano(),
		endTime.UnixNano(),
		tsn,
		dataNo,
	}
	if modelSerial != "" {
		sb.WriteString(` and MODL_SERIAL = ?`)
		args = append(args, modelSerial)
	}
	if areaCode != "" {
		sb.WriteString(` and AREA_CODE = ?`)
		args = append(args, areaCode)
	}
	rows, err := conn.Query(c, sb.String(), args...)
	if err != nil {
		defaultLog.Errorf("Failed to query database: %v", err)
		c.JSON(http.StatusInternalServerError, ApiErrorServer)
		return
	}
	defer rows.Close()

	c.Header("Content-Type", "application/json")
	c.Writer.WriteString(`{`)
	fmt.Fprintf(c.Writer, `"dataNo":"%d",`, dataNo)
	fmt.Fprintf(c.Writer, `"datasetNo":"%d",`, tsn)
	fmt.Fprintf(c.Writer, `"resultCode":"SUCC-000","resultMsg":"전송 완료",`)
	fmt.Fprintf(c.Writer, `"startDateTime":"%s",`, startTime.Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"endDateTime":"%s",`, endTime.Format("20060102150405"))
	fmt.Fprintf(c.Writer, `"resultdata":[`)
	for rows.Next() {
		var seq int64
		var modelSerial string
		var date time.Time
		var areaCode string
		var packet string

		if err := rows.Scan(&seq, &modelSerial, &date, &areaCode, &packet); err != nil {
			defaultLog.Errorf("Failed to scan row: %v", err)
			c.JSON(http.StatusInternalServerError, ApiErrorServer)
			return
		}
		if nrow > 0 {
			c.Writer.WriteString(",")
		}
		fmt.Fprintf(c.Writer, `{"seq":%d,"serial":"%s","date":"%s","areaCode":"%s","packet":"%s"}`,
			seq, modelSerial, date.Format("2006-01-02 15:04:05"), areaCode, packet)
		nrow++
	}
	c.Writer.WriteString(`]}`)
	return
}
