package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/anishathalye/porcupine"
)

const historySchemaVersion = "fast-caspaxos-porcupine-history-v1"

type historyArtifact struct {
	SchemaVersion string         `json:"SchemaVersion"`
	ModelKind     string         `json:"ModelKind"`
	ScenarioName  string         `json:"ScenarioName"`
	Round         int            `json:"Round"`
	Seed          int            `json:"Seed"`
	Events        []historyEvent `json:"Events"`
}

type historyEvent struct {
	Sequence      int64           `json:"Sequence"`
	Kind          string          `json:"Kind"`
	OperationID   int             `json:"OperationId"`
	RequestID     int             `json:"RequestId"`
	ClientID      int             `json:"ClientId"`
	Client        string          `json:"Client"`
	Proposer      string          `json:"Proposer"`
	Input         *operationInput `json:"Input"`
	Output        *valueOutput    `json:"Output"`
	ProtocolRound *int            `json:"ProtocolRound"`
}

type operationInput struct {
	Kind            string  `json:"Kind"`
	ExpectedVersion *int    `json:"ExpectedVersion"`
	Value           *string `json:"Value"`
}

type valueOutput struct {
	Version     int      `json:"Version"`
	StringValue *string  `json:"StringValue"`
	SetValue    []string `json:"SetValue"`
}

type checkResult struct {
	ScenarioName string
	ModelKind    string
	Round        int
	Seed         int
	EventCount   int
	Linearizable bool
	HistoryPath  string
}

type stringInput struct {
	Kind            string
	ExpectedVersion int
	Value           string
}

type stringState struct {
	Version int
	Value   string
}

type setInput struct {
	Kind  string
	Value string
}

type setState struct {
	Version int
	Values  []string
}

func loadHistory(path string) (historyArtifact, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return historyArtifact{}, fmt.Errorf("read history: %w", err)
	}

	var history historyArtifact
	if err := json.Unmarshal(contents, &history); err != nil {
		return historyArtifact{}, fmt.Errorf("parse history: %w", err)
	}

	if history.SchemaVersion != historySchemaVersion {
		return historyArtifact{}, fmt.Errorf("unsupported history schema %q", history.SchemaVersion)
	}

	if len(history.Events) == 0 {
		return historyArtifact{}, errors.New("history does not contain any events")
	}

	return history, nil
}

func checkHistoryPath(path string) (checkResult, error) {
	history, err := loadHistory(path)
	if err != nil {
		return checkResult{}, err
	}

	result, err := checkHistory(history)
	if err != nil {
		return checkResult{}, err
	}

	result.HistoryPath = path
	return result, nil
}

func collectHistoryPaths(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat history path: %w", err)
	}

	if !info.IsDir() {
		return []string{path}, nil
	}

	var historyPaths []string
	err = filepath.WalkDir(path, func(currentPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			return nil
		}

		if entry.Name() == "porcupine-history.json" {
			historyPaths = append(historyPaths, currentPath)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk history directory: %w", err)
	}

	sort.Strings(historyPaths)
	if len(historyPaths) == 0 {
		return nil, errors.New("no porcupine-history.json artifacts found")
	}

	return historyPaths, nil
}

func checkHistory(history historyArtifact) (checkResult, error) {
	model, events, err := buildModelAndEvents(history)
	if err != nil {
		return checkResult{}, err
	}

	return checkResult{
		ScenarioName: history.ScenarioName,
		ModelKind:    history.ModelKind,
		Round:        history.Round,
		Seed:         history.Seed,
		EventCount:   len(events),
		Linearizable: porcupine.CheckEvents(model, events),
	}, nil
}

func buildModelAndEvents(history historyArtifact) (porcupine.Model, []porcupine.Event, error) {
	var model porcupine.Model
	switch history.ModelKind {
	case "string":
		model = buildStringModel()
	case "set":
		model = buildSetModel()
	default:
		return porcupine.Model{}, nil, fmt.Errorf("unsupported model kind %q", history.ModelKind)
	}

	sortedEvents := slices.Clone(history.Events)
	sort.Slice(sortedEvents, func(i, j int) bool {
		return sortedEvents[i].Sequence < sortedEvents[j].Sequence
	})

	events := make([]porcupine.Event, 0, len(sortedEvents))
	for _, event := range sortedEvents {
		converted, err := convertEvent(history.ModelKind, event)
		if err != nil {
			return porcupine.Model{}, nil, err
		}

		events = append(events, converted)
	}

	return model, events, nil
}

func convertEvent(modelKind string, event historyEvent) (porcupine.Event, error) {
	base := porcupine.Event{
		ClientId: event.ClientID,
		Id:       event.OperationID,
	}

	switch event.Kind {
	case "call":
		if event.Input == nil {
			return porcupine.Event{}, fmt.Errorf("call event %d is missing input", event.OperationID)
		}

		input, err := convertInput(modelKind, *event.Input)
		if err != nil {
			return porcupine.Event{}, err
		}

		base.Kind = porcupine.CallEvent
		base.Value = input
		return base, nil
	case "return":
		if event.Output == nil {
			return porcupine.Event{}, fmt.Errorf("return event %d is missing output", event.OperationID)
		}

		output, err := convertOutput(modelKind, *event.Output)
		if err != nil {
			return porcupine.Event{}, err
		}

		base.Kind = porcupine.ReturnEvent
		base.Value = output
		return base, nil
	default:
		return porcupine.Event{}, fmt.Errorf("unsupported event kind %q", event.Kind)
	}
}

func convertInput(modelKind string, input operationInput) (interface{}, error) {
	switch modelKind {
	case "string":
		value := derefString(input.Value)
		expectedVersion := 0
		if input.ExpectedVersion != nil {
			expectedVersion = *input.ExpectedVersion
		}

		return stringInput{
			Kind:            input.Kind,
			ExpectedVersion: expectedVersion,
			Value:           value,
		}, nil
	case "set":
		return setInput{
			Kind:  input.Kind,
			Value: derefString(input.Value),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported model kind %q", modelKind)
	}
}

func convertOutput(modelKind string, output valueOutput) (interface{}, error) {
	switch modelKind {
	case "string":
		return stringState{
			Version: output.Version,
			Value:   derefString(output.StringValue),
		}, nil
	case "set":
		values := slices.Clone(output.SetValue)
		sort.Strings(values)
		return setState{
			Version: output.Version,
			Values:  values,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported model kind %q", modelKind)
	}
}

func buildStringModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return stringState{}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			current := state.(stringState)
			request := input.(stringInput)
			actual := output.(stringState)
			next, err := applyStringOperation(current, request)
			if err != nil {
				return false, current
			}

			return next == actual, next
		},
		Equal: func(left interface{}, right interface{}) bool {
			return left.(stringState) == right.(stringState)
		},
	}
}

func buildSetModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return setState{}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			current := state.(setState)
			request := input.(setInput)
			actual := output.(setState)
			next, err := applySetOperation(current, request)
			if err != nil {
				return false, current
			}

			return setStatesEqual(next, actual), next
		},
		Equal: func(left interface{}, right interface{}) bool {
			return setStatesEqual(left.(setState), right.(setState))
		},
	}
}

func applyStringOperation(current stringState, request stringInput) (stringState, error) {
	switch request.Kind {
	case "append_character":
		return stringState{
			Version: current.Version + 1,
			Value:   current.Value + request.Value,
		}, nil
	case "append_at_version":
		if request.ExpectedVersion == current.Version+1 {
			return stringState{
				Version: request.ExpectedVersion,
				Value:   current.Value + request.Value,
			}, nil
		}

		return current, nil
	case "read":
		return current, nil
	default:
		return stringState{}, fmt.Errorf("unsupported string operation kind %q", request.Kind)
	}
}

func applySetOperation(current setState, request setInput) (setState, error) {
	switch request.Kind {
	case "add":
		if slices.Contains(current.Values, request.Value) {
			return current, nil
		}

		nextValues := slices.Clone(current.Values)
		nextValues = append(nextValues, request.Value)
		sort.Strings(nextValues)
		return setState{
			Version: current.Version + 1,
			Values:  nextValues,
		}, nil
	case "read":
		return current, nil
	default:
		return setState{}, fmt.Errorf("unsupported set operation kind %q", request.Kind)
	}
}

func setStatesEqual(left setState, right setState) bool {
	if left.Version != right.Version {
		return false
	}

	return slices.Equal(left.Values, right.Values)
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}
