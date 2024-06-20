package main

import "testing"

func TestCheckHistory_StringHistoryPasses(t *testing.T) {
	history := historyArtifact{
		SchemaVersion: historySchemaVersion,
		ModelKind:     "string",
		ScenarioName:  "string-corpus",
		Round:         1,
		Seed:          7001,
		Events: []historyEvent{
			callEvent(1, 1, operationInput{Kind: "append_character", Value: stringPtr("A")}),
			returnStringEvent(2, 1, stringState{Version: 1, Value: "A"}),
			callEvent(3, 2, operationInput{Kind: "read"}),
			returnStringEvent(4, 2, stringState{Version: 1, Value: "A"}),
		},
	}

	result, err := checkHistory(history)
	if err != nil {
		t.Fatalf("checkHistory returned error: %v", err)
	}

	if !result.Linearizable {
		t.Fatalf("expected linearizable history")
	}
}

func TestCheckHistory_StringHistoryFails(t *testing.T) {
	history := historyArtifact{
		SchemaVersion: historySchemaVersion,
		ModelKind:     "string",
		ScenarioName:  "string-invalid",
		Round:         1,
		Seed:          7002,
		Events: []historyEvent{
			callEvent(1, 1, operationInput{Kind: "append_character", Value: stringPtr("A")}),
			callEvent(2, 2, operationInput{Kind: "read"}),
			returnStringEvent(3, 2, stringState{Version: 1, Value: "A"}),
			callEvent(4, 3, operationInput{Kind: "read"}),
			returnStringEvent(5, 3, stringState{}),
			returnStringEvent(6, 1, stringState{Version: 1, Value: "A"}),
		},
	}

	result, err := checkHistory(history)
	if err != nil {
		t.Fatalf("checkHistory returned error: %v", err)
	}

	if result.Linearizable {
		t.Fatalf("expected non-linearizable history")
	}
}

func TestCheckHistory_SetHistoryPasses(t *testing.T) {
	history := historyArtifact{
		SchemaVersion: historySchemaVersion,
		ModelKind:     "set",
		ScenarioName:  "set-corpus",
		Round:         1,
		Seed:          7003,
		Events: []historyEvent{
			callEvent(1, 1, operationInput{Kind: "add", Value: stringPtr("A")}),
			returnSetEvent(2, 1, setState{Version: 1, Values: []string{"A"}}),
			callEvent(3, 2, operationInput{Kind: "add", Value: stringPtr("A")}),
			returnSetEvent(4, 2, setState{Version: 1, Values: []string{"A"}}),
			callEvent(5, 3, operationInput{Kind: "read"}),
			returnSetEvent(6, 3, setState{Version: 1, Values: []string{"A"}}),
		},
	}

	result, err := checkHistory(history)
	if err != nil {
		t.Fatalf("checkHistory returned error: %v", err)
	}

	if !result.Linearizable {
		t.Fatalf("expected linearizable history")
	}
}

func callEvent(sequence int64, operationID int, input operationInput) historyEvent {
	return historyEvent{
		Sequence:    sequence,
		Kind:        "call",
		OperationID: operationID,
		RequestID:   operationID,
		ClientID:    operationID,
		Client:      "proposer",
		Proposer:    "proposer",
		Input:       &input,
	}
}

func returnStringEvent(sequence int64, operationID int, state stringState) historyEvent {
	return historyEvent{
		Sequence:    sequence,
		Kind:        "return",
		OperationID: operationID,
		RequestID:   operationID,
		ClientID:    operationID,
		Client:      "proposer",
		Proposer:    "proposer",
		Output: &valueOutput{
			Version:     state.Version,
			StringValue: stringPtr(state.Value),
		},
	}
}

func returnSetEvent(sequence int64, operationID int, state setState) historyEvent {
	return historyEvent{
		Sequence:    sequence,
		Kind:        "return",
		OperationID: operationID,
		RequestID:   operationID,
		ClientID:    operationID,
		Client:      "proposer",
		Proposer:    "proposer",
		Output: &valueOutput{
			Version:  state.Version,
			SetValue: state.Values,
		},
	}
}

func stringPtr(value string) *string {
	return &value
}
