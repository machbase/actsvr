package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/magefile/mage/sh"
)

func BuildAll() error {
	fmt.Println("Building all binaries...")
	binaries := []string{"loader", "siotdata", "csvrev"}
	for _, bin := range binaries {
		if err := Build(bin); err != nil {
			return fmt.Errorf("failed to build %s: %w", bin, err)
		}
	}
	return nil
}

func Build(bin string) error {
	fmt.Println("Building:", bin)
	env := map[string]string{
		"GO111MODULE": "on",
		"CGO_ENABLED": "1",
	}

	if runtime.GOOS == "linux" {
		env["CGO_LDFLAGS"] = "-pthread"
	}

	tz, _ := time.LoadLocation("Asia/Seoul")
	flagX := fmt.Sprintf("-X main.version=%s", time.Now().In(tz).Format("20060102.150405"))
	args := []string{"build",
		"-ldflags", flagX,
		"-o", "./tmp/" + bin,
		"./cmd/" + bin,
	}
	return sh.RunWithV(env, "go", args...)
}

func Protoc() error {
	protoFiles := []struct {
		srcDir string
		proto  string
		dstDir string
	}{
		{srcDir: "./greetings", proto: "greetings.proto", dstDir: "./greetings"},
		{srcDir: "./trjd", proto: "trjd.proto", dstDir: "./trjd"},
		{srcDir: "./loader", proto: "loader.proto", dstDir: "./loader"},
	}

	for _, file := range protoFiles {
		fmt.Printf("protoc regen ./msg/%s...\n", file.proto)
		sh.RunV("protoc",
			"-I", file.srcDir,
			fmt.Sprintf("--go_out=%s", file.dstDir), "--go_opt=paths=source_relative",
			fmt.Sprintf("--go-grpc_out=%s", file.dstDir), "--go-grpc_opt=paths=source_relative",
			"--experimental_allow_proto3_optional",
			file.proto,
		)
	}
	return nil
}
