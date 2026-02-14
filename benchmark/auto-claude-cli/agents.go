package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

func (r *runner) runClaudePrompt(prompt string) error {
	return r.runAgentPromptWithRetry("claude", []string{"-p", "--dangerously-skip-permissions"}, prompt, "Claude")
}

func (r *runner) runGeminiPrompt(prompt string) error {
	return r.runAgentPromptWithRetry("gemini", []string{"-p", prompt}, "", "Gemini")
}

func (r *runner) runAgentPromptWithRetry(bin string, args []string, prompt, label string) error {
	for attempt := 1; attempt <= r.cfg.maxUsageRetries; attempt++ {
		r.log(fmt.Sprintf("%s invocation (attempt %d / %d)", label, attempt, r.cfg.maxUsageRetries))
		cmd := exec.Command(bin, args...)
		if prompt != "" {
			cmd.Stdin = strings.NewReader(prompt)
		}

		var buf bytes.Buffer
		w := io.MultiWriter(os.Stdout, &buf)
		cmd.Stdout = w
		cmd.Stderr = w
		err := cmd.Run()
		_ = os.WriteFile(r.agentOut, buf.Bytes(), 0o644)
		output := buf.String()

		if err == nil {
			return nil
		}
		if !usageExhausted(output) {
			return fmt.Errorf("%s exited with error: %w", strings.ToLower(label), err)
		}

		wait := parseLimitResetWait(output, r.cfg.usageWait)
		resume := time.Now().Add(wait).Format("15:04:05")
		r.log(fmt.Sprintf("%s limit hit. Sleeping %s until ~%s.", label, wait.Round(time.Second), resume))
		if sleepErr := sleepWithCaffeinate(wait); sleepErr != nil {
			time.Sleep(wait)
		}
	}
	return fmt.Errorf("exhausted %d usage-wait retries for %s", r.cfg.maxUsageRetries, label)
}
