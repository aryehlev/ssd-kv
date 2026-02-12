package main

import "time"

type config struct {
	maxFixRounds    int
	crPollInterval  time.Duration
	crTimeout       time.Duration
	crSettleTime    time.Duration
	usageWait       time.Duration
	maxUsageRetries int
	maxGeminiFixes  int
	maxBenchFixes   int
}

type runner struct {
	cfg config

	task       string
	baseBranch string
	branch     string

	tmpDir   string
	planFile string
	agentOut string
	prURL    string
	repo     string
	owner    string
	repoName string
}
