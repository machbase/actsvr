package main

import (
	"fmt"

	"github.com/magefile/mage/sh"
)

func Build(bin string) error {
	fmt.Println("Building:", bin)
	return sh.RunV("go", "build", "-o", "./tmp/"+bin, "./cmd/"+bin)
}

func Protoc() error {
	protoFiles := []struct {
		srcDir string
		proto  string
		dstDir string
	}{
		{srcDir: "./greetings", proto: "greetings.proto", dstDir: "./greetings"},
		{srcDir: "./trjd", proto: "trjd.proto", dstDir: "./trjd"},
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
