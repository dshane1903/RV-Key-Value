package store

import (
	"encoding/json"
	"fmt"
)

type Operation string

const (
	OperationPut    Operation = "put"
	OperationDelete Operation = "delete"
	OperationNoop   Operation = "noop"
)

type Command struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
}

func EncodeCommand(command Command) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(command)
}

func DecodeCommand(data []byte) (Command, error) {
	var command Command
	if err := json.Unmarshal(data, &command); err != nil {
		return Command{}, err
	}
	if err := command.Validate(); err != nil {
		return Command{}, err
	}
	return command, nil
}

func (c Command) Validate() error {
	if c.Operation == OperationNoop {
		return nil
	}
	if c.Key == "" {
		return fmt.Errorf("kv command key is required")
	}

	switch c.Operation {
	case OperationPut, OperationDelete:
		return nil
	default:
		return fmt.Errorf("unsupported kv operation %q", c.Operation)
	}
}
