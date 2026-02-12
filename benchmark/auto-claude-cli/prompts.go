package main

import "fmt"

func (r *runner) buildTaskPrompt() string {
	return fmt.Sprintf(`%s

Additional required deliverables (do all of these before finishing):
1. Run benchmark comparisons for:
   - Aerospike vs SSD-KV
   - Valkey vs SSD-KV in-memory/Redis-compatible mode
2. Use benchmark results to improve SSD-KV performance and keep iterating until it is as close as possible to Valkey/Aerospike under the measured workloads.
3. If you add new command types, data types, or workloads in code, update benchmark tooling and scripts so they benchmark those additions too.
   Relevant files to update as needed include:
   - benchmark/run_comparison.sh
   - benchmark/run_redis_benchmark.sh
   - benchmark/run_redis_benchmark.go
   - benchmark/redis_vs_aerospike.rs
4. Include a concise summary of benchmark commands run, key throughput/latency results, and what performance fixes were made.

Do not ask questions. Make decisions and execute end-to-end.
`, r.task)
}

func (r *runner) planPrompt() string {
	return fmt.Sprintf(`Create a detailed implementation plan for this task:

%s

Requirements:
1. Explore the codebase.
2. Write a concrete, step-by-step plan.
3. Save the plan using the Write tool to exactly this path: %s
4. Include files to modify, exact changes, ordering, edge cases, tests, linting, and benchmark steps.

Only write the plan. Do not implement yet. Do not ask questions.
`, r.buildTaskPrompt(), r.planFile)
}

func (r *runner) implementPrompt() string {
	return fmt.Sprintf(`Read and execute this plan in full: %s

Rules:
1. Execute every step in the plan.
2. Make all code changes directly.
3. Run required checks and benchmarks from the plan.
4. Do not create a new plan.
5. Do not ask questions.
`, r.planFile)
}
