package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (r *runner) log(msg string) {
	fmt.Printf("\n=== [%s] %s ===\n\n", time.Now().Format("15:04:05"), msg)
}

func hasChanges() (bool, error) {
	out, err := runCmdCapture("git", "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func usageExhausted(out string) bool {
	re := regexp.MustCompile(`(?i)(rate.?limit|usage.?limit|too many requests|quota.*(exceeded|reached)|out of usage|exceeded.*limit|429|try again later|capacity|overloaded|billing|throttl)`)
	return re.MatchString(out)
}

func parseLimitResetWait(out string, fallback time.Duration) time.Duration {
	lower := strings.ToLower(out)

	reMins := regexp.MustCompile(`(?i)(?:in|after)\s+(\d+)\s*(minute|minutes|min|m)\b`)
	if m := reMins.FindStringSubmatch(lower); len(m) == 3 {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			return time.Duration(n) * time.Minute
		}
	}

	reSecs := regexp.MustCompile(`(?i)(?:in|after)\s+(\d+)\s*(second|seconds|sec|s)\b`)
	if m := reSecs.FindStringSubmatch(lower); len(m) == 3 {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}

	reHours := regexp.MustCompile(`(?i)(?:in|after)\s+(\d+)\s*(hour|hours|hr|h)\b`)
	if m := reHours.FindStringSubmatch(lower); len(m) == 3 {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			return time.Duration(n) * time.Hour
		}
	}

	reClock := regexp.MustCompile(`(?i)(?:reset|try again|available).{0,40}\b(\d{1,2}):(\d{2})\b`)
	if m := reClock.FindStringSubmatch(lower); len(m) == 3 {
		h, errH := strconv.Atoi(m[1])
		min, errM := strconv.Atoi(m[2])
		if errH == nil && errM == nil && h >= 0 && h <= 23 && min >= 0 && min <= 59 {
			now := time.Now()
			reset := time.Date(now.Year(), now.Month(), now.Day(), h, min, 0, 0, now.Location())
			if !reset.After(now) {
				reset = reset.Add(24 * time.Hour)
			}
			return reset.Sub(now)
		}
	}

	return fallback
}

func sleepWithCaffeinate(wait time.Duration) error {
	secs := int(wait.Seconds())
	if secs <= 0 {
		return nil
	}
	cmd := exec.Command("caffeinate", "-i", "sleep", strconv.Itoa(secs))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runCmdCapture(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s failed: %w (%s)", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func runCmdStream(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func runCmdStreamCapture(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var buf bytes.Buffer
	w := io.MultiWriter(os.Stdout, &buf)
	cmd.Stdout = w
	cmd.Stderr = w
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	return buf.String(), err
}

func clippyPanicked(output string) bool {
	re := regexp.MustCompile(`(?i)(query stack during panic|internal compiler error|rustc unexpectedly panicked|clippy.*panicked|thread 'rustc'.*panicked)`)
	return re.MatchString(output)
}

func parseInt(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q: %w", s, err)
	}
	return i, nil
}

func extractPRNumber(prURL string) (int, error) {
	re := regexp.MustCompile(`/pull/(\\d+)$`)
	m := re.FindStringSubmatch(strings.TrimSpace(prURL))
	if len(m) != 2 {
		return 0, fmt.Errorf("unable to parse PR number from URL: %s", prURL)
	}
	return strconv.Atoi(m[1])
}

func headChars(s string, n int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n])
}
