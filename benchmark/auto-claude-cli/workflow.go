package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (r *runner) run() error {
	if err := r.initRuntime(); err != nil {
		return err
	}
	defer os.RemoveAll(r.tmpDir)

	r.log("Phase 1: Create implementation plan")
	if err := r.runClaudePrompt(r.planPrompt()); err != nil {
		return err
	}
	if _, err := os.Stat(r.planFile); err != nil {
		return fmt.Errorf("Claude did not produce a plan at %s", r.planFile)
	}
	fmt.Printf("Plan written to %s\n", r.planFile)

	r.log("Phase 2: Execute plan in new Claude session")
	if err := r.runClaudePrompt(r.implementPrompt()); err != nil {
		return err
	}

	r.log("Phase 3: Branch, lint/bench, push, create PR")
	changed, err := hasChanges()
	if err != nil {
		return err
	}
	if !changed {
		return errors.New("no changes detected, nothing to commit")
	}

	if err := r.createBranchAndInitialPR(); err != nil {
		return err
	}

	prNum, err := extractPRNumber(r.prURL)
	if err != nil {
		return err
	}
	r.log(fmt.Sprintf("PR #%d created: %s", prNum, r.prURL))

	r.log(fmt.Sprintf("Phase 4: CodeRabbit review-fix loop (max %d rounds)", r.cfg.maxFixRounds))
	if err := r.runCodeRabbitLoop(prNum); err != nil {
		return err
	}

	r.log("Workflow complete")
	fmt.Printf("PR: %s\n", r.prURL)
	fmt.Printf("Branch: %s\n", r.branch)
	return nil
}

func (r *runner) initRuntime() error {
	if _, err := os.Getwd(); err != nil {
		return err
	}

	r.branch = "claude/" + time.Now().Format("20060102-150405")

	tmpDir, err := os.MkdirTemp("", "auto-claude-")
	if err != nil {
		return err
	}
	r.tmpDir = tmpDir
	r.planFile = filepath.Join(tmpDir, "plan.md")
	r.agentOut = filepath.Join(tmpDir, "agent-output.txt")

	return r.initRepoContext()
}

func (r *runner) initRepoContext() error {
	repo, err := runCmdCapture("gh", "repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner")
	if err != nil {
		return fmt.Errorf("failed to resolve repo via gh: %w", err)
	}
	r.repo = strings.TrimSpace(repo)

	parts := strings.SplitN(r.repo, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("unexpected repo nameWithOwner format: %q", r.repo)
	}
	r.owner = parts[0]
	r.repoName = parts[1]
	return nil
}

func (r *runner) createBranchAndInitialPR() error {
	if err := runCmdStream("git", "checkout", "-b", r.branch); err != nil {
		return err
	}
	if err := r.runPreCommitChecks(); err != nil {
		return err
	}
	if err := runCmdStream("git", "add", "-A"); err != nil {
		return err
	}
	if err := runCmdStream("git", "commit", "-m", "feat: automated implementation"); err != nil {
		return err
	}
	if err := runCmdStream("git", "push", "-u", "origin", r.branch); err != nil {
		return err
	}

	prURL, err := runCmdCapture("gh", "pr", "create",
		"--base", r.baseBranch,
		"--title", fmt.Sprintf("feat: %s", headChars(r.task, 60)),
		"--body", r.prBody(),
	)
	if err != nil {
		return err
	}
	r.prURL = strings.TrimSpace(prURL)
	return nil
}

func (r *runner) runCodeRabbitLoop(prNum int) error {
	for round := 1; round <= r.cfg.maxFixRounds; round++ {
		before, err := r.crReviewCount(prNum)
		if err != nil {
			return err
		}

		reviewArrived, err := r.waitForCodeRabbit(prNum, before)
		if err != nil {
			return err
		}
		if !reviewArrived {
			fmt.Println("No CodeRabbit review arrived, exiting fix loop.")
			break
		}

		unresolved, err := r.crUnresolvedThreads(prNum)
		if err != nil {
			return err
		}
		fmt.Printf("  Unresolved CodeRabbit threads: %d\n", unresolved)
		if unresolved == 0 {
			r.log("All CodeRabbit comments resolved, nothing to fix")
			break
		}

		r.log(fmt.Sprintf("Fix round %d / %d (%d unresolved threads)", round, r.cfg.maxFixRounds, unresolved))
		fixPrompt := fmt.Sprintf("Fix the latest unresolved CodeRabbit PR comments for PR #%d. Apply fixes directly, then stop.\n", prNum)
		if err := r.runClaudePrompt(fixPrompt); err != nil {
			return err
		}

		changed, err := hasChanges()
		if err != nil {
			return err
		}
		if !changed {
			fmt.Printf("  No file changes after fix round %d, stopping.\n", round)
			break
		}

		if err := r.runPreCommitChecks(); err != nil {
			return err
		}
		if err := runCmdStream("git", "add", "-A"); err != nil {
			return err
		}
		if err := runCmdStream("git", "commit", "-m", fmt.Sprintf("fix: address CodeRabbit review (round %d)", round)); err != nil {
			return err
		}
		if err := runCmdStream("git", "push"); err != nil {
			return err
		}

		if round == r.cfg.maxFixRounds {
			fmt.Printf("  Reached max fix rounds (%d).\n", r.cfg.maxFixRounds)
		}
	}
	return nil
}

func (r *runner) runPreCommitChecks() error {
	if err := r.runStrictLintAndGeminiFixes(); err != nil {
		return err
	}
	return r.runBenchmarksAndGeminiFixes()
}

func (r *runner) prBody() string {
	plan, _ := os.ReadFile(r.planFile)
	if strings.TrimSpace(string(plan)) == "" {
		return fmt.Sprintf(`## Summary
%s

---
Generated with [auto-claude-cli](benchmark/auto-claude-cli) automated workflow.
`, r.task)
	}

	return fmt.Sprintf(`## Summary
%s

## Implementation Plan
<details>
<summary>Click to expand plan</summary>

%s

</details>

---
Generated with [auto-claude-cli](benchmark/auto-claude-cli) automated workflow.
`, r.task, string(plan))
}
