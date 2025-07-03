package main

import (
	"fmt"

	"github.com/magefile/mage/sh"
)

func BuildGreetings() error {
	fmt.Println("Building greetings...")
	return Build("greetings", "greetings")
}

func Build(tags string, bin string) error {
	fmt.Println("Building with tags:", tags, "and output binary:", bin)
	return sh.RunV("go", "build", "-tags", tags, "-o", "./tmp/"+bin)
}

func Protoc() error {
	protoFiles := []struct {
		srcDir string
		proto  string
		dstDir string
	}{
		{srcDir: "./greetings", proto: "greetings.proto", dstDir: "./greetings"},
	}

	for _, file := range protoFiles {
		fmt.Printf("protoc regen ./msg/%s...\n", file.proto)
		sh.RunV("protoc", "-I", file.srcDir, file.proto,
			"--experimental_allow_proto3_optional",
			fmt.Sprintf("--go_out=%s", file.dstDir), "--go_opt=paths=source_relative",
			fmt.Sprintf("--go-grpc_out=%s", file.dstDir), "--go-grpc_opt=paths=source_relative",
		)
	}
	return nil
}
