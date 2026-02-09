#!/usr/bin/env bash
#
# auto-claude.sh — Fully automated Claude Code workflow with CodeRabbit review loop
#
# Usage:
#   ./auto-claude.sh "Your task description here" [base-branch]
#
# Workflow:
#   1. Claude explores the codebase and writes a plan to a file
#   2. Claude reads the plan and implements it
#   3. Script creates branch, commits, pushes, opens a PR
#   4. Waits for CodeRabbit to review
#   5. Claude fixes review comments via /fix-pr-comments
#   6. Repeats 4-5 until no unresolved comments remain (or max rounds hit)
#

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────
MAX_FIX_ROUNDS=5           # max review-fix iterations
CR_POLL_INTERVAL=30        # seconds between CodeRabbit polls
CR_TIMEOUT=900             # 15 min max wait per review cycle
CR_SETTLE_TIME=20          # extra seconds after review detected to let it finish
USAGE_WAIT=3600            # seconds to sleep when usage exhausted (1 hour)
MAX_USAGE_RETRIES=10       # max times to retry after usage exhaustion per invocation

# ── Input validation ─────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 \"<task description>\" [base-branch]"
    exit 1
fi

TASK="$1"
BASE_BRANCH="${2:-master}"

# ── Derived values ───────────────────────────────────────────────────
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
OWNER="${REPO%%/*}"
REPO_NAME="${REPO##*/}"
BRANCH="claude/$(date +%Y%m%d-%H%M%S)"
WORK_DIR="$PWD"
TMPDIR=$(mktemp -d)
PLAN_FILE="$TMPDIR/plan.md"

trap 'rm -rf "$TMPDIR"' EXIT

# ── Helpers ──────────────────────────────────────────────────────────
log() {
    printf '\n\033[1;34m=== [%s] %s ===\033[0m\n\n' "$(date +%H:%M:%S)" "$*"
}

# Count reviews posted by CodeRabbit on a PR
cr_review_count() {
    gh api "repos/$REPO/pulls/$1/reviews" \
        --jq '[.[] | select(.user.login == "coderabbitai[bot]")] | length' 2>/dev/null || echo 0
}

# Count unresolved review threads authored by CodeRabbit (GraphQL)
cr_unresolved_threads() {
    gh api graphql -f query='
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
      }' -f owner="$OWNER" -f repo="$REPO_NAME" -F pr="$1" \
      --jq '
        [.data.repository.pullRequest.reviewThreads.nodes[]
         | select(.isResolved == false
                  and .comments.nodes[0].author.login == "coderabbitai[bot]")]
        | length
      ' 2>/dev/null || echo 0
}

# Block until a new CodeRabbit review appears (or timeout)
wait_for_coderabbit() {
    local pr="$1" before="$2" elapsed=0

    log "Waiting for CodeRabbit review on PR #$pr …"
    while [ "$elapsed" -lt "$CR_TIMEOUT" ]; do
        sleep "$CR_POLL_INTERVAL"
        elapsed=$((elapsed + CR_POLL_INTERVAL))

        local current
        current=$(cr_review_count "$pr")
        if [ "$current" -gt "$before" ]; then
            echo "  CodeRabbit review detected — letting it finish posting…"
            sleep "$CR_SETTLE_TIME"
            return 0
        fi
        echo "  Still waiting… (${elapsed}s / ${CR_TIMEOUT}s)"
    done

    echo "  Timed out waiting for CodeRabbit review."
    return 1
}

run_claude() {
    local prompt_file="$1"
    local attempt=0
    local output_file="$TMPDIR/claude-output.txt"

    while [ "$attempt" -lt "$MAX_USAGE_RETRIES" ]; do
        attempt=$((attempt + 1))
        log "Claude invocation (attempt $attempt / $MAX_USAGE_RETRIES)"

        local exit_code=0
        # --dangerously-skip-permissions: no confirmation prompts
        # -p (--print): non-interactive, outputs result to stdout
        # Capture both stdout+stderr; tee to terminal for visibility
        claude -p --dangerously-skip-permissions < "$prompt_file" 2>&1 | tee "$output_file" || exit_code=$?

        # Check for usage/rate-limit exhaustion in output or non-zero exit
        if [ "$exit_code" -ne 0 ] && usage_exhausted "$output_file"; then
            local resume_time
            resume_time=$(date -v+${USAGE_WAIT}S +%H:%M:%S 2>/dev/null || date -d "+${USAGE_WAIT} seconds" +%H:%M:%S 2>/dev/null || echo "~1 hour")
            log "Usage/rate limit hit. Sleeping ${USAGE_WAIT}s until ~${resume_time} (attempt $attempt). Mac will stay awake via caffeinate."
            caffeinate -i sleep "$USAGE_WAIT"
            continue
        fi

        # Non-rate-limit failure — bail out
        if [ "$exit_code" -ne 0 ]; then
            echo "ERROR: Claude exited with code $exit_code (not a rate limit). Aborting."
            return "$exit_code"
        fi

        # Success
        return 0
    done

    echo "ERROR: Exhausted $MAX_USAGE_RETRIES usage-wait retries. Aborting."
    return 1
}

# Detect whether Claude output indicates usage/rate-limit exhaustion
usage_exhausted() {
    local output_file="$1"
    # Match common rate-limit / usage-limit phrases (case-insensitive)
    grep -qiE \
        'rate.?limit|usage.?limit|too many requests|quota.*(exceeded|reached)|out of usage|exceeded.*limit|429|try again later|capacity|overloaded|billing|throttl' \
        "$output_file" 2>/dev/null
}

# ═════════════════════════════════════════════════════════════════════
# Phase 1 — Plan
# ═════════════════════════════════════════════════════════════════════
log "Phase 1: Creating implementation plan"

PROMPT_FILE="$TMPDIR/prompt-plan.txt"
cat > "$PROMPT_FILE" <<PROMPT
I need you to create a detailed implementation plan for the following task:

$TASK

Steps to follow:
1. Explore the codebase to understand the architecture and all relevant files.
2. Write a comprehensive step-by-step implementation plan.
3. Save the plan to this file using the Write tool: $PLAN_FILE

The plan MUST include:
- Every file to create or modify, with rationale
- The specific changes for each file (detailed enough to implement without ambiguity)
- The correct order of operations
- Edge cases, pitfalls, and testing considerations

Only write the plan — do NOT implement anything yet.
Do not ask any questions at any time! just keep running and make desicions!
PROMPT

run_claude "$PROMPT_FILE"

if [ ! -f "$PLAN_FILE" ]; then
    echo "ERROR: Claude did not produce a plan at $PLAN_FILE — aborting."
    exit 1
fi
echo "Plan written to $PLAN_FILE"

# ═════════════════════════════════════════════════════════════════════
# Phase 2 — Implement
# ═════════════════════════════════════════════════════════════════════
log "Phase 2: Implementing the plan"

PROMPT_FILE="$TMPDIR/prompt-impl.txt"
cat > "$PROMPT_FILE" <<PROMPT
Read the implementation plan at: $PLAN_FILE

Implement every step in full. Make all code changes described in the plan.
Do NOT skip any step. Do NOT create a new plan — just implement the existing one.
After all changes are made, verify the code compiles (if applicable).
Do not ask any questions at any time! just keep running and make desicions!

PROMPT

run_claude "$PROMPT_FILE"

# ═════════════════════════════════════════════════════════════════════
# Phase 3 — Branch → commit → push → open PR
# ═════════════════════════════════════════════════════════════════════
log "Phase 3: Pushing to branch and opening PR"

# Bail if nothing changed
if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
    echo "No changes detected — nothing to commit. Aborting."
    exit 1
fi

git checkout -b "$BRANCH"
git add -A
git commit -m "$(cat <<'EOF'
feat: automated implementation

EOF
)"
git push -u origin "$BRANCH"

PR_URL=$(gh pr create \
    --base "$BASE_BRANCH" \
    --title "feat: $(echo "$TASK" | head -c 60)" \
    --body "$(cat <<EOF
## Summary
$TASK

## Implementation Plan
<details>
<summary>Click to expand plan</summary>

$(cat "$PLAN_FILE")

</details>

---
Generated with [auto-claude.sh](auto-claude.sh) automated workflow.
EOF
)")

PR_NUMBER=$(echo "$PR_URL" | grep -oE '[0-9]+$')
log "PR #$PR_NUMBER created: $PR_URL"

# ═════════════════════════════════════════════════════════════════════
# Phase 4 — CodeRabbit review → fix loop
# ═════════════════════════════════════════════════════════════════════
log "Phase 4: CodeRabbit review-fix loop (max $MAX_FIX_ROUNDS rounds)"

for round in $(seq 1 "$MAX_FIX_ROUNDS"); do
    before=$(cr_review_count "$PR_NUMBER")

    if ! wait_for_coderabbit "$PR_NUMBER" "$before"; then
        echo "No CodeRabbit review arrived — exiting fix loop."
        break
    fi

    unresolved=$(cr_unresolved_threads "$PR_NUMBER")
    echo "  Unresolved CodeRabbit threads: $unresolved"

    if [ "$unresolved" -eq 0 ]; then
        log "All CodeRabbit comments resolved — nothing to fix!"
        break
    fi

    log "Fix round $round / $MAX_FIX_ROUNDS ($unresolved unresolved threads)"

    PROMPT_FILE="$TMPDIR/prompt-fix.txt"
    cat > "$PROMPT_FILE" <<PROMPT
/fix-pr-comments $PR_NUMBER
PROMPT

    run_claude "$PROMPT_FILE"

    # Commit & push if there are changes
    if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
        echo "  No file changes after fix round $round — stopping."
        break
    fi

    git add -A
    git commit -m "$(cat <<EOF
fix: address CodeRabbit review (round $round)

EOF
)"
    git push

    if [ "$round" -eq "$MAX_FIX_ROUNDS" ]; then
        echo "  Reached max fix rounds ($MAX_FIX_ROUNDS)."
    fi
done

# ═════════════════════════════════════════════════════════════════════
# Done
# ═════════════════════════════════════════════════════════════════════
log "Workflow complete!"
echo "PR: $PR_URL"
echo "Branch: $BRANCH"
