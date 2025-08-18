package siotdata

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func Main() int {
	var input string
	var output string = "-"
	var clients int = 1
	var apiServer string = "http://127.0.0.1:5680"

	// RDB configuration
	flag.StringVar(&rdbConfig.host, "rdb-host", rdbConfig.host, "RDB host")
	flag.IntVar(&rdbConfig.port, "rdb-port", rdbConfig.port, "RDB port")
	flag.StringVar(&rdbConfig.user, "rdb-user", rdbConfig.user, "RDB user")
	flag.StringVar(&rdbConfig.pass, "rdb-pass", rdbConfig.pass, "RDB password")
	flag.StringVar(&rdbConfig.db, "rdb-db", rdbConfig.db, "RDB database")
	flag.IntVar(&clients, "clients", clients, "Number of clients to run simultaneously")
	// api server
	flag.StringVar(&apiServer, "api-server", apiServer, "API server URL")
	// input & output
	flag.StringVar(&input, "input", "", "Input file path")
	flag.StringVar(&output, "output", output, "Output file path")
	flag.Parse()

	// cache
	certKeys = make(map[string]*Certkey)

	// load certkeys
	var db, err = rdbConfig.Connect()
	if err != nil {
		fmt.Printf("Failed to connect to RDB: %v\n", err)
		return 1
	}
	defer db.Close()

	SelectCertkey(db, func(ck *Certkey) bool {
		if ck.TrnsmitServerNo.Valid {
			certKeys[fmt.Sprintf("%d", ck.TrnsmitServerNo.Int64)] = ck
		}
		return true
	})

	// open input file
	if input == "" {
		fmt.Printf("Input file path is required\n")
		return 1
	}
	inputFile, err := os.Open(input)
	if err != nil {
		fmt.Printf("Failed to open input file: %v\n", err)
		return 1
	}
	defer inputFile.Close()

	// create clients
	clientC := make(chan interface{}, clients)
	for i := 0; i < clients; i++ {
		clientC <- struct{}{}
	}
	// run clients
	csvReader := csv.NewReader(inputFile)
	wg := sync.WaitGroup{}
	apiServer = strings.TrimSuffix(apiServer, "/")
	out := os.Stdout
	start := time.Now()
	cnt := 0
	if output != "-" {
		outFile, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Printf("Failed to open output file: %v\n", err)
			return 1
		}
		defer outFile.Close()
		out = outFile
	}
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Failed to read CSV record: %v\n", err)
			return 1
		}

		// 0:PACKET_SEQ, 1:TRNSMIT_SERVER_NO, 2:DATA_NO, 3:PK_SEQ, 4:MODL_SERIAL
		// 5:PACKET
		// 6:PACKET_STTUS_CODE, 7:RECPTN_RESULT_CODE, 8:RECPTN_RESULT_MSSAGE, 9:PARS_SE_CODE, 10:PARS_DT
		// 11:REGIST_DE, 12:REGIST_TIME, 13:REGIST_DT, 14:AREA_CODE
		//
		// Process the record
		pkSeq := record[3]
		modelSerial := record[4]
		data := record[5]

		tsn := record[1]
		certKey, ok := certKeys[tsn]
		if !ok {
			fmt.Fprintf(out, "No certkey found for TRNSMIT_SERVER_NO %s\n", tsn)
			continue
		}
		urlPath, err := url.Parse(fmt.Sprintf("%s/n/api/send/%s/1/%s/%s/%s",
			apiServer, certKey.CrtfcKey.String, pkSeq, modelSerial, data))
		if err != nil {
			fmt.Fprintln(out, "Failed to parse URL:",
				err, apiServer, certKey.CrtfcKey.String, pkSeq, modelSerial, data)
			continue
		}
		// wait for available client
		<-clientC
		wg.Add(1)
		go func(urlPath *url.URL) {
			defer func() {
				clientC <- struct{}{}
				wg.Done()
			}()
			req := &http.Request{
				Method: "GET",
				URL:    urlPath,
			}
			rsp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Fprintf(out, "Failed to send request: %v, %s\n", err, urlPath.String())
				return
			}
			defer rsp.Body.Close()
			if rsp.StatusCode != http.StatusOK {
				msg, _ := io.ReadAll(rsp.Body)
				fmt.Fprintf(out, "Failed to send request: %v, %s\n",
					strings.TrimSpace(string(msg)), urlPath.String())
				return
			}
		}(urlPath)
		cnt++
	}

	wg.Wait()

	// print cnt with comma formating
	pr := message.NewPrinter(language.Korean)
	pr.Fprintf(out, "Processed %d requests in %v\n", cnt, time.Since(start))
	return 0
}
