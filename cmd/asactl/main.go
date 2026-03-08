package main

import (
	"context"
	"fmt"
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
	if err := cli.NewRootCommand(buildVersion()).ExecuteContext(context.Background()); err != nil {
		os.Exit(1)
	}
}
