#!/bin/bash

PROMPT_FILE="prompt.txt"
DELAY=$((5 * 60 * 60))   # 5 hours in seconds

if [ ! -f "$PROMPT_FILE" ]; then
  echo "Missing prompt.txt"
  exit 1
fi

echo "Keeping Mac awake and running Claude in 5 hours…"

# Keep Mac awake a bit longer than the delay
caffeinate -t $((DELAY + 300)) &

# Wait 5 hours
sleep $DELAY

# Run Claude
claude --dangerously-skip-permissions < "$PROMPT_FILE"
