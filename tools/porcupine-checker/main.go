package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var historyPath string
	flag.StringVar(&historyPath, "history", "", "Path to a porcupine-history.json artifact or a directory containing them")
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
	for _, candidate := range historyPaths {
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
	os.Exit(exitCode)
}
