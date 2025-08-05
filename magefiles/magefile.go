package main

import (
	"fmt"
	"runtime"

	"github.com/magefile/mage/sh"
)

func BuildAll() error {
	fmt.Println("Building all binaries...")
	binaries := []string{"loader", "siotsvr"}
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

	return sh.RunWithV(env, "go", "build", "-o", "./tmp/"+bin, "./cmd/"+bin)
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
