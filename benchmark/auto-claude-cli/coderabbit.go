package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (r *runner) crReviewCount(pr int) (int, error) {
	out, err := runCmdCapture("gh", "api", fmt.Sprintf("repos/%s/pulls/%d/reviews", r.repo, pr), "--jq", `[.[] | select(.user.login == "coderabbitai[bot]")] | length`)
	if err != nil {
		return 0, nil
	}
	return parseInt(strings.TrimSpace(out))
}

func (r *runner) crUnresolvedThreads(pr int) (int, error) {
	query := `
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          comments(first: 1) {
            nodes { author { login } }
          }
        }
      }
    }
  }
}`

	out, err := runCmdCapture(
		"gh", "api", "graphql",
		"-f", "query="+query,
		"-f", "owner="+r.owner,
		"-f", "repo="+r.repoName,
		"-F", "pr="+strconv.Itoa(pr),
		"--jq", `[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false and .comments.nodes[0].author.login == "coderabbitai[bot]")] | length`,
	)
	if err != nil {
		return 0, nil
	}
	return parseInt(strings.TrimSpace(out))
}

func (r *runner) waitForCodeRabbit(pr, before int) (bool, error) {
	r.log(fmt.Sprintf("Waiting for CodeRabbit review on PR #%d ...", pr))
	elapsed := time.Duration(0)

	for elapsed < r.cfg.crTimeout {
		time.Sleep(r.cfg.crPollInterval)
		elapsed += r.cfg.crPollInterval

		current, err := r.crReviewCount(pr)
		if err != nil {
			return false, err
		}
		if current > before {
			fmt.Println("  CodeRabbit review detected, letting it finish posting...")
			time.Sleep(r.cfg.crSettleTime)
			return true, nil
		}
		fmt.Printf("  Still waiting... (%ds / %ds)\n", int(elapsed.Seconds()), int(r.cfg.crTimeout.Seconds()))
	}

	fmt.Println("  Timed out waiting for CodeRabbit review.")
	return false, nil
}
