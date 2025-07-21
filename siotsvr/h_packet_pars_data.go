package siotsvr

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) writePacketParsData(c *gin.Context) {
	var data PacketParsData
	var registDT string
	existingColumns := map[string]string{}
	if c.Request.Method == http.MethodGet {
		if err := c.ShouldBindQuery(&data); err != nil {
			s.log.Println("Error binding query parameters:", err)
			c.String(http.StatusBadRequest, "Invalid query parameters, %s", err.Error())
			return
		}
		registDT, _ = c.GetQuery("REGIST_DT")
		for i := 0; i < 60; i++ {
			columnName := fmt.Sprintf("COLUMN%d", i)
			if value, exists := c.GetQuery(columnName); exists {
				existingColumns[columnName] = value
			}
		}
	} else {
		if err := c.ShouldBindJSON(&data); err != nil {
			s.log.Println("Error binding JSON body:", err)
			c.String(http.StatusBadRequest, "Invalid JSON body, %s", err.Error())
			return
		}
		registDT = c.PostForm("REGIST_DT")
		for i := 0; i < 60; i++ {
			columnName := fmt.Sprintf("COLUMN%d", i)
			if value, exists := c.GetPostForm(columnName); exists {
				existingColumns[columnName] = value
			}
		}
	}

	if registDT != "" {
		// Parse the date string into a time.Time object
		parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", registDT, DefaultLocation)
		if err != nil {
			s.log.Println("Error parsing REGIST_DT:", err)
			c.String(http.StatusBadRequest, "Invalid REGIST_DT format, must be '2006-01-02 15:04:05'")
			return
		}
		data.RegistDt = parsedTime
	}
	// Validate the data
	if err := data.Validate(); err != nil {
		s.log.Println("Validation error:", err)
		c.String(http.StatusBadRequest, fmt.Sprintf("Validation failed: %s", err.Error()))
		return
	}

	// sort columns to ensure consistent order
	columnKeys := make([]string, 0, len(existingColumns))
	for k := range existingColumns {
		columnKeys = append(columnKeys, k)
	}
	slices.Sort(columnKeys)

	conn, err := s.openConn(c)
	if err != nil {
		c.String(http.StatusInternalServerError, "Database connection error")
		return
	}
	defer conn.Close()

	sqlBuilder := strings.Builder{}
	sqlBuilder.WriteString(`INSERT INTO TB_PACKET_PARS_DATA(`)
	sqlBuilder.WriteString(`PACKET_PARS_SEQ,`)
	sqlBuilder.WriteString(`PACKET_SEQ,`)
	sqlBuilder.WriteString(`TRNSMIT_SERVER_NO,`)
	sqlBuilder.WriteString(`REGIST_DT,`)
	sqlBuilder.WriteString(`REGIST_DE,`)
	sqlBuilder.WriteString(`SERVICE_SEQ,`)
	sqlBuilder.WriteString(`AREA_CODE,`)
	sqlBuilder.WriteString(`MODL_SERIAL`)
	for _, col := range columnKeys {
		sqlBuilder.WriteString(fmt.Sprintf(",%s", col))
	}
	sqlBuilder.WriteString(`) VALUES(?,?,?,?,?,?,?,?`)
	for range columnKeys {
		sqlBuilder.WriteString(",?")
	}
	sqlBuilder.WriteString(`)`)

	values := []interface{}{
		data.PacketParsSeq,
		data.PacketSeq,
		data.TransmitServerNo,
		data.RegistDt,
		data.RegistDe,
		data.ServiceSeq,
		data.AreaCode,
		data.ModlSerial,
	}
	for _, col := range columnKeys {
		values = append(values, existingColumns[col])
	}
	result := conn.Exec(c, sqlBuilder.String(), values...)
	if result.Err() != nil {
		c.String(http.StatusInternalServerError, "Failed to write PacketParsData, "+result.Err().Error())
		return
	}
	c.String(http.StatusOK, "PacketParsData written successfully!")
}

type PacketParsData struct {
	PacketParsSeq    int64     `form:"PACKET_PARS_SEQ" binding:"required"`
	PacketSeq        int64     `form:"PACKET_SEQ" binding:"required"`
	TransmitServerNo int       `form:"TRNSMIT_SERVER_NO" json:"TRNSMIT_SERVER_NO"` // TODO: Add binding:"required"
	RegistDt         time.Time `form:"REGIST_DT" binding:"required" time_format:"2006-01-02 15:04:05"`
	RegistDe         string    `form:"REGIST_DE" binding:"required"`
	ServiceSeq       int       `form:"SERVICE_SEQ" binding:"required"`
	AreaCode         string    `form:"AREA_CODE" binding:"required"`
	ModlSerial       string    `form:"MODL_SERIAL" binding:"required"`
	Column0          string    `json:"COLUMN0,omitempty"`
	Column1          string    `json:"COLUMN1,omitempty"`
	Column2          string    `json:"COLUMN2,omitempty"`
	Column3          string    `json:"COLUMN3,omitempty"`
	Column4          string    `json:"COLUMN4,omitempty"`
	Column5          string    `json:"COLUMN5,omitempty"`
	Column6          string    `json:"COLUMN6,omitempty"`
	Column7          string    `json:"COLUMN7,omitempty"`
	Column8          string    `json:"COLUMN8,omitempty"`
	Column9          string    `json:"COLUMN9,omitempty"`
	Column10         string    `json:"COLUMN10,omitempty"`
	Column11         string    `json:"COLUMN11,omitempty"`
	Column12         string    `json:"COLUMN12,omitempty"`
	Column13         string    `json:"COLUMN13,omitempty"`
	Column14         string    `json:"COLUMN14,omitempty"`
	Column15         string    `json:"COLUMN15,omitempty"`
	Column16         string    `json:"COLUMN16,omitempty"`
	Column17         string    `json:"COLUMN17,omitempty"`
	Column18         string    `json:"COLUMN18,omitempty"`
	Column19         string    `json:"COLUMN19,omitempty"`
	Column20         string    `json:"COLUMN20,omitempty"`
	Column21         string    `json:"COLUMN21,omitempty"`
	Column22         string    `json:"COLUMN22,omitempty"`
	Column23         string    `json:"COLUMN23,omitempty"`
	Column24         string    `json:"COLUMN24,omitempty"`
	Column25         string    `json:"COLUMN25,omitempty"`
	Column26         string    `json:"COLUMN26,omitempty"`
	Column27         string    `json:"COLUMN27,omitempty"`
	Column28         string    `json:"COLUMN28,omitempty"`
	Column29         string    `json:"COLUMN29,omitempty"`
	Column30         string    `json:"COLUMN30,omitempty"`
	Column31         string    `json:"COLUMN31,omitempty"`
	Column32         string    `json:"COLUMN32,omitempty"`
	Column33         string    `json:"COLUMN33,omitempty"`
	Column34         string    `json:"COLUMN34,omitempty"`
	Column35         string    `json:"COLUMN35,omitempty"`
	Column36         string    `json:"COLUMN36,omitempty"`
	Column37         string    `json:"COLUMN37,omitempty"`
	Column38         string    `json:"COLUMN38,omitempty"`
	Column39         string    `json:"COLUMN39,omitempty"`
	Column40         string    `json:"COLUMN40,omitempty"`
	Column41         string    `json:"COLUMN41,omitempty"`
	Column42         string    `json:"COLUMN42,omitempty"`
	Column43         string    `json:"COLUMN43,omitempty"`
	Column44         string    `json:"COLUMN44,omitempty"`
	Column45         string    `json:"COLUMN45,omitempty"`
	Column46         string    `json:"COLUMN46,omitempty"`
	Column47         string    `json:"COLUMN47,omitempty"`
	Column48         string    `json:"COLUMN48,omitempty"`
	Column49         string    `json:"COLUMN49,omitempty"`
	Column50         string    `json:"COLUMN50,omitempty"`
	Column51         string    `json:"COLUMN51,omitempty"`
	Column52         string    `json:"COLUMN52,omitempty"`
	Column53         string    `json:"COLUMN53,omitempty"`
	Column54         string    `json:"COLUMN54,omitempty"`
	Column55         string    `json:"COLUMN55,omitempty"`
	Column56         string    `json:"COLUMN56,omitempty"`
	Column57         string    `json:"COLUMN57,omitempty"`
	Column58         string    `json:"COLUMN58,omitempty"`
	Column59         string    `json:"COLUMN59,omitempty"`
}

func (data *PacketParsData) Validate() error {
	// Validate PacketParsSeq - should be positive
	if data.PacketParsSeq <= 0 {
		return fmt.Errorf("invalid PACKET_PARS_SEQ: %d, must be greater than 0", data.PacketParsSeq)
	}

	// Validate PacketSeq - should be positive
	if data.PacketSeq <= 0 {
		return fmt.Errorf("invalid PACKET_SEQ: %d, must be greater than 0", data.PacketSeq)
	}

	return nil
}
