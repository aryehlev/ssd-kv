package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	cfg := config{
		maxFixRounds:    5,
		crPollInterval:  30 * time.Second,
		crTimeout:       15 * time.Minute,
		crSettleTime:    20 * time.Second,
		usageWait:       1 * time.Hour,
		maxUsageRetries: 10,
		maxGeminiFixes:  3,
		maxBenchFixes:   2,
	}

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s \"<task description>\" [base-branch]\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	r := &runner{
		cfg:        cfg,
		task:       os.Args[1],
		baseBranch: "master",
	}
	if len(os.Args) >= 3 {
		r.baseBranch = os.Args[2]
	}

	if err := r.run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}
