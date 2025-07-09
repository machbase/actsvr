package trjd

import (
	"actsvr/util"
	"context"
	"embed"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"
	"github.com/tochemey/goakt/v3/log"
)

//go:embed static
var htmlFS embed.FS

func (s *HttpServer) Router() *gin.Engine {
	if s.log.LogLevel() == log.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/static/")
	})
	r.GET("/static/", gin.WrapF(http.FileServerFS(htmlFS).ServeHTTP))
	r.POST("/uploads", gin.WrapF(s.uploadFile))
	r.GET("/welcome", func(c *gin.Context) {
		c.String(http.StatusOK, "Welcome to the HTTP server!")
	})
	return r
}

func (s *HttpServer) uploadFile(w http.ResponseWriter, r *http.Request) {
	s.log.Println("File Upload Endpoint Hit")

	// Parse multipart form, 10 << 20 specifies a maximum
	// upload of 10 MB files.
	r.ParseMultipartForm(10 << 20)

	// upload_file
	file, handler, err := r.FormFile("upload_file")
	if err != nil {
		s.log.Println("Error Retrieving the File")
		s.log.Println(err)
		return
	}
	defer file.Close()
	s.log.Printf("Uploaded File: %+v", handler.Filename)
	s.log.Printf("File Size: %+v", handler.Size)
	s.log.Printf("MIME Header: %+v", handler.Header)

	// Create a temporary file within temp directory
	// that follows a particular naming pattern
	tempFile, err := os.CreateTemp(s.TempDir, "upload-*")
	if err != nil {
		s.log.Println(err)
		return
	}
	// copy uploaded file into a temp file
	if _, err := io.Copy(tempFile, file); err != nil {
		s.log.Println("Error saving the file")
		s.log.Println(err)
		return
	}
	tempFile.Close()

	// return that we have successfully uploaded our file!
	fmt.Fprintf(w, "Successfully Uploaded File\n")

	im := &Importer{
		req: &ImportRequest{
			WorkId:   NewWorkId().String(),
			Src:      tempFile.Name(),
			DstHost:  s.dbHost,
			DstPort:  int32(s.dbPort),
			DstUser:  s.dbUser,
			DstPass:  s.dbPass,
			DstTable: s.dbTable,
		},
	}

	s.actorSystem.Spawn(r.Context(), im.req.WorkId, im)
}

type Importer struct {
	req *ImportRequest
	log *util.Log
}

func (im *Importer) PreStart(ctx *actor.Context) error {
	im.log = ctx.ActorSystem().Logger().(*util.Log)
	return nil
}

func (im *Importer) PostStop(ctx *actor.Context) error {
	return nil
}

func (im *Importer) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goaktpb.PostStart:
		_, master, err := ctx.ActorSystem().ActorOf(ctx.Context(), MasterName)
		if err != nil {
			im.log.Println("Error getting Master actor:", err)
			return
		}

		err = actor.Tell(context.Background(), master, im.req)
		if err != nil {
			im.log.Println("Error sending import request to Master actor:", err)
			return
		}
		im.log.Println("Unhandled message type:", msg)
	default:
		ctx.Unhandled()
	}
}
