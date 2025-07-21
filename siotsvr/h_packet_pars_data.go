package siotsvr

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const maxColumns = 64 // Maximum number of columns

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
		for i := 0; i < maxColumns; i++ {
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
		for i := 0; i < maxColumns; i++ {
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
	Column0          string    `form:"COLUMN0"`
	Column1          string    `form:"COLUMN1"`
	Column2          string    `form:"COLUMN2"`
	Column3          string    `form:"COLUMN3"`
	Column4          string    `form:"COLUMN4"`
	Column5          string    `form:"COLUMN5"`
	Column6          string    `form:"COLUMN6"`
	Column7          string    `form:"COLUMN7"`
	Column8          string    `form:"COLUMN8"`
	Column9          string    `form:"COLUMN9"`
	Column10         string    `form:"COLUMN10"`
	Column11         string    `form:"COLUMN11"`
	Column12         string    `form:"COLUMN12"`
	Column13         string    `form:"COLUMN13"`
	Column14         string    `form:"COLUMN14"`
	Column15         string    `form:"COLUMN15"`
	Column16         string    `form:"COLUMN16"`
	Column17         string    `form:"COLUMN17"`
	Column18         string    `form:"COLUMN18"`
	Column19         string    `form:"COLUMN19"`
	Column20         string    `form:"COLUMN20"`
	Column21         string    `form:"COLUMN21"`
	Column22         string    `form:"COLUMN22"`
	Column23         string    `form:"COLUMN23"`
	Column24         string    `form:"COLUMN24"`
	Column25         string    `form:"COLUMN25"`
	Column26         string    `form:"COLUMN26"`
	Column27         string    `form:"COLUMN27"`
	Column28         string    `form:"COLUMN28"`
	Column29         string    `form:"COLUMN29"`
	Column30         string    `form:"COLUMN30"`
	Column31         string    `form:"COLUMN31"`
	Column32         string    `form:"COLUMN32"`
	Column33         string    `form:"COLUMN33"`
	Column34         string    `form:"COLUMN34"`
	Column35         string    `form:"COLUMN35"`
	Column36         string    `form:"COLUMN36"`
	Column37         string    `form:"COLUMN37"`
	Column38         string    `form:"COLUMN38"`
	Column39         string    `form:"COLUMN39"`
	Column40         string    `form:"COLUMN40"`
	Column41         string    `form:"COLUMN41"`
	Column42         string    `form:"COLUMN42"`
	Column43         string    `form:"COLUMN43"`
	Column44         string    `form:"COLUMN44"`
	Column45         string    `form:"COLUMN45"`
	Column46         string    `form:"COLUMN46"`
	Column47         string    `form:"COLUMN47"`
	Column48         string    `form:"COLUMN48"`
	Column49         string    `form:"COLUMN49"`
	Column50         string    `form:"COLUMN50"`
	Column51         string    `form:"COLUMN51"`
	Column52         string    `form:"COLUMN52"`
	Column53         string    `form:"COLUMN53"`
	Column54         string    `form:"COLUMN54"`
	Column55         string    `form:"COLUMN55"`
	Column56         string    `form:"COLUMN56"`
	Column57         string    `form:"COLUMN57"`
	Column58         string    `form:"COLUMN58"`
	Column59         string    `form:"COLUMN59"`
	Column60         string    `form:"COLUMN60"`
	Column61         string    `form:"COLUMN61"`
	Column62         string    `form:"COLUMN62"`
	Column63         string    `form:"COLUMN63"`
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
