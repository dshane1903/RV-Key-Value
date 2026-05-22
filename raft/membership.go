package raft

import (
	"encoding/json"
	"fmt"
	"strings"
)

const membershipCommandOperation = "raft_membership"

type MembershipChangeType string

const (
	MembershipAddPeer    MembershipChangeType = "add_peer"
	MembershipRemovePeer MembershipChangeType = "remove_peer"
)

type MembershipChange struct {
	Type   MembershipChangeType `json:"type"`
	PeerID string               `json:"peer_id"`
}

type membershipCommand struct {
	Operation string               `json:"operation"`
	Type      MembershipChangeType `json:"type"`
	PeerID    string               `json:"peer_id"`
}

func EncodeMembershipChange(change MembershipChange) ([]byte, error) {
	if err := change.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(membershipCommand{
		Operation: membershipCommandOperation,
		Type:      change.Type,
		PeerID:    change.PeerID,
	})
}

func DecodeMembershipChange(data []byte) (MembershipChange, bool, error) {
	var command membershipCommand
	if err := json.Unmarshal(data, &command); err != nil {
		return MembershipChange{}, false, nil
	}
	if command.Operation != membershipCommandOperation {
		return MembershipChange{}, false, nil
	}

	change := MembershipChange{
		Type:   command.Type,
		PeerID: command.PeerID,
	}
	if err := change.Validate(); err != nil {
		return MembershipChange{}, true, err
	}
	return change, true, nil
}

func (c MembershipChange) Validate() error {
	if strings.TrimSpace(c.PeerID) == "" {
		return fmt.Errorf("membership peer id is required")
	}
	if strings.TrimSpace(c.PeerID) != c.PeerID {
		return fmt.Errorf("membership peer id must not contain surrounding whitespace")
	}

	switch c.Type {
	case MembershipAddPeer, MembershipRemovePeer:
		return nil
	default:
		return fmt.Errorf("unsupported membership change %q", c.Type)
	}
}
