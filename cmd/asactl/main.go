package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/robaerd/asactl/internal/cli"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func buildVersion() string {
	trimmedVersion := strings.TrimSpace(version)
	if trimmedVersion == "" {
		trimmedVersion = "dev"
	}
	if trimmedVersion == "dev" {
		return trimmedVersion
	}
	shortCommit := strings.TrimSpace(commit)
	if len(shortCommit) > 7 {
		shortCommit = shortCommit[:7]
	}
	trimmedDate := strings.TrimSpace(date)
	if shortCommit == "" || shortCommit == "none" || trimmedDate == "" || trimmedDate == "unknown" {
		return trimmedVersion
	}
	return fmt.Sprintf("%s (%s, %s)", trimmedVersion, shortCommit, trimmedDate)
}

func main() {
	os.Exit(run(context.Background(), os.Stdout, os.Stderr, os.Args[1:]))
}

func run(ctx context.Context, stdout, stderr io.Writer, args []string) int {
	cmd := cli.NewRootCommand(buildVersion())
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs(args)
	if err := cmd.ExecuteContext(ctx); err != nil {
		cli.PrintError(stderr, err)
		return 1
	}
	return 0
}
