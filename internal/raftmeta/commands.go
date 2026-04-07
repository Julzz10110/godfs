package raftmeta

import (
	"encoding/json"
)

type commandType string

const (
	cmdRegisterNode commandType = "register_node"
	cmdMkdir        commandType = "mkdir"
	cmdCreateFile   commandType = "create_file"
	cmdRename       commandType = "rename"
	cmdDelete       commandType = "delete"
	cmdPrepareWrite commandType = "prepare_write"
	cmdCommitChunk  commandType = "commit_chunk"
	cmdHeartbeat    commandType = "heartbeat"
	cmdAddReplica   commandType = "add_replica"
	cmdClearPendingDeleteAddr commandType = "clear_pending_delete_addr"
	cmdMarkPendingDeleteAttempt commandType = "mark_pending_delete_attempt"
)

type commandEnvelope struct {
	Type commandType       `json:"type"`
	Data json.RawMessage   `json:"data"`
}

func encodeCommand(t commandType, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	env := commandEnvelope{Type: t, Data: b}
	return json.Marshal(&env)
}

func decodeEnvelope(b []byte) (commandEnvelope, error) {
	var env commandEnvelope
	err := json.Unmarshal(b, &env)
	return env, err
}

