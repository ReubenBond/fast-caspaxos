package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	var historyPath string
	flag.StringVar(&historyPath, "history", "", "Path to a porcupine-history.json, .jsonl file, or directory containing them")
	flag.Parse()

	if historyPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -history argument")
		os.Exit(2)
	}

	historyPaths, err := collectHistoryPaths(historyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	exitCode := 0
	total := 0
	for _, candidate := range historyPaths {
		if filepath.Ext(candidate) == ".jsonl" {
			n, code := checkJSONLFile(candidate)
			total += n
			if code != 0 {
				exitCode = code
			}
		} else {
			total++
			result, err := checkHistoryPath(candidate)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}

			status := "PASS"
			if !result.Linearizable {
				status = "FAIL"
				exitCode = 1
			}

			fmt.Printf(
				"%s %s round=%d seed=%d model=%s events=%d\n",
				status,
				result.ScenarioName,
				result.Round,
				result.Seed,
				result.ModelKind,
				result.EventCount)
		}
	}
	fmt.Printf("\n%d histories checked\n", total)
	os.Exit(exitCode)
}

// checkJSONLFile streams a JSONL file line-by-line, checking each history.
// Returns the number of histories checked and a non-zero exit code on failure.
func checkJSONLFile(path string) (int, int) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open JSONL file: %v\n", err)
		os.Exit(2)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Allow up to 10 MB per line for large histories.
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	lineNum := 0
	exitCode := 0
	passed := 0
	failed := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var history historyArtifact
		if err := json.Unmarshal(line, &history); err != nil {
			fmt.Fprintf(os.Stderr, "%s:%d: parse error: %v\n", path, lineNum, err)
			os.Exit(2)
		}

		if history.SchemaVersion != historySchemaVersion {
			fmt.Fprintf(os.Stderr, "%s:%d: unsupported schema %q\n", path, lineNum, history.SchemaVersion)
			os.Exit(2)
		}

		if len(history.Events) == 0 {
			fmt.Fprintf(os.Stderr, "%s:%d: history has no events\n", path, lineNum)
			os.Exit(2)
		}

		result, err := checkHistory(history)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s:%d: %v\n", path, lineNum, err)
			os.Exit(2)
		}

		if result.Linearizable {
			passed++
		} else {
			failed++
			exitCode = 1
			fmt.Printf(
				"FAIL %s round=%d seed=%d model=%s events=%d (line %d)\n",
				result.ScenarioName,
				result.Round,
				result.Seed,
				result.ModelKind,
				result.EventCount,
				lineNum)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read JSONL file: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf(
		"JSONL %s: %d passed, %d failed (%d total)\n",
		filepath.Base(path), passed, failed, passed+failed)

	return passed + failed, exitCode
}
