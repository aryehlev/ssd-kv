package main

import "fmt"

func (r *runner) runStrictLintAndGeminiFixes() error {
	r.log("Running strict Rust lint checks before commit")

	for attempt := 0; attempt <= r.cfg.maxGeminiFixes; attempt++ {
		// Avoid known clippy panic paths on test targets by linting non-test
		// targets strictly, then compiling tests separately.
		err := r.runClippyStrictNoTests()
		if err == nil {
			if testErr := runCmdStream("cargo", "test", "--all-features", "--no-run"); testErr != nil {
				return fmt.Errorf("test compile check failed: %w", testErr)
			}
			return nil
		}
		if attempt == r.cfg.maxGeminiFixes {
			return fmt.Errorf("strict clippy failed after %d gemini attempts: %w", r.cfg.maxGeminiFixes, err)
		}

		r.log(fmt.Sprintf("Lint failed. Asking Gemini to fix issues (attempt %d / %d)", attempt+1, r.cfg.maxGeminiFixes))
		prompt := "Fix all Rust lint errors in this repository so these commands pass: " +
			"`cargo clippy --workspace --all-features --lib --bins --examples --benches -- -D warnings -D clippy::pedantic -D clippy::nursery -D clippy::cargo` " +
			"and `cargo test --all-features --no-run`. " +
			"Apply fixes directly to files and keep behavior intact."
		if gemErr := r.runGeminiPrompt(prompt); gemErr != nil {
			return fmt.Errorf("gemini failed while fixing lint errors: %w", gemErr)
		}
	}
	return nil
}

func (r *runner) runClippyStrictNoTests() error {
	strictArgs := []string{
		"clippy", "--workspace", "--all-features", "--lib", "--bins", "--examples", "--benches", "--",
		"-D", "warnings",
		"-D", "clippy::pedantic",
		"-D", "clippy::nursery",
		"-D", "clippy::cargo",
	}

	output, err := runCmdStreamCapture("cargo", strictArgs...)
	if err == nil {
		return nil
	}
	if !clippyPanicked(output) {
		return err
	}

	r.log("Detected Clippy panic with full strict profile. Retrying with panic-safe strict subset.")
	fallbackArgs := []string{
		"clippy", "--workspace", "--all-features", "--lib", "--bins", "--examples", "--benches", "--",
		"-D", "warnings",
		"-D", "clippy::pedantic",
	}
	fallbackOut, fallbackErr := runCmdStreamCapture("cargo", fallbackArgs...)
	if fallbackErr == nil {
		return nil
	}
	if clippyPanicked(fallbackOut) {
		return fmt.Errorf("clippy panicked under both strict and fallback profiles; likely toolchain issue")
	}
	return fallbackErr
}

func (r *runner) runBenchmarksAndGeminiFixes() error {
	r.log("Running benchmark checks before commit")

	for attempt := 0; attempt <= r.cfg.maxBenchFixes; attempt++ {
		err := r.runRequiredBenchmarks()
		if err == nil {
			return nil
		}
		if attempt == r.cfg.maxBenchFixes {
			return fmt.Errorf("benchmarks failed after %d gemini attempts: %w", r.cfg.maxBenchFixes, err)
		}

		r.log(fmt.Sprintf("Benchmarks failed. Asking Gemini to fix issues (attempt %d / %d)", attempt+1, r.cfg.maxBenchFixes))
		prompt := "Fix benchmark failures for SSD-KV comparisons. Ensure this flow works: " +
			"benchmark/run_redis_benchmark.sh for Valkey/Redis-compatible comparison. " +
			"Update benchmark scripts if new command/data types need coverage."
		if gemErr := r.runGeminiPrompt(prompt); gemErr != nil {
			return fmt.Errorf("gemini failed while fixing benchmark issues: %w", gemErr)
		}
	}
	return nil
}

func (r *runner) runRequiredBenchmarks() error {
	if err := runCmdStream("bash", "benchmark/run_redis_benchmark.sh"); err != nil {
		return fmt.Errorf("valkey/redis benchmark failed: %w", err)
	}
	return nil
}
