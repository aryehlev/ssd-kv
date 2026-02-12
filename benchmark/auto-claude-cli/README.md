# auto-claude-cli

Automated workflow CLI split by responsibility:

- `main.go`: entrypoint and args
- `types.go`: config and runner types
- `workflow.go`: orchestration (plan/implement/PR/fix loop)
- `prompts.go`: Claude prompt builders
- `agents.go`: Claude/Gemini execution + limit retry/sleep logic
- `checks.go`: strict lint + benchmark gates and Gemini fix loops
- `coderabbit.go`: CodeRabbit polling/query helpers
- `utils.go`: shared helpers

Run:

```bash
cd /Users/aryehlev/Documents/ssd-kv/benchmark/auto-claude-cli
go run . "<task description>" [base-branch]
```
