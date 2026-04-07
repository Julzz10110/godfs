package raftmeta

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"godfs/internal/domain"
)

// FSM applies Raft log entries to metadata State.
type FSM struct {
	mu sync.RWMutex
	st *State
}

func NewFSM(initial *State) *FSM {
	if initial == nil {
		panic("initial state required")
	}
	return &FSM{st: initial}
}

func (f *FSM) State() *State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// Return pointer for read-only access by callers under their own discipline.
	return f.st
}

func (f *FSM) Apply(l *raft.Log) any {
	env, err := decodeEnvelope(l.Data)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch env.Type {
	case cmdRegisterNode:
		var req domain.ChunkNode
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		return f.st.RegisterNode(req)

	case cmdMkdir:
		var req struct{ Path string }
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		return f.st.Mkdir(req.Path)

	case cmdCreateFile:
		var req struct {
			Path   string
			FileID domain.FileID
			AtUnix int64
		}
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		_, err := f.st.CreateFile(req.Path, req.FileID, time.Unix(req.AtUnix, 0).UTC())
		return err

	case cmdRename:
		var req struct {
			Old string
			New string
		}
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		return f.st.Rename(req.Old, req.New)

	case cmdDelete:
		var req struct{ Path string }
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		infos, err := f.st.Delete(req.Path)
		if err != nil {
			return err
		}
		return infos

	case cmdPrepareWrite:
		var req struct {
			Path      string
			Offset    int64
			Length    int64
			LeaseID   domain.LeaseID
			NewChunkID domain.ChunkID
			AtUnix    int64
		}
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		res, err := f.st.PrepareWrite(req.Path, req.Offset, req.Length, req.LeaseID, req.NewChunkID, time.Unix(req.AtUnix, 0).UTC())
		if err != nil {
			return err
		}
		return res

	case cmdCommitChunk:
		var req struct {
			Path       string
			ChunkID    domain.ChunkID
			ChunkIndex int64
			ChunkOffset int64
			Written    int64
			Checksum   []byte
			Version    uint64
			AtUnix     int64
		}
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		return f.st.CommitChunk(req.Path, req.ChunkID, req.ChunkIndex, req.ChunkOffset, req.Written, req.Checksum, req.Version, time.Unix(req.AtUnix, 0).UTC())

	case cmdHeartbeat:
		var req struct {
			NodeID       domain.NodeID
			CapacityBytes int64
			UsedBytes    int64
			AtUnix       int64
		}
		if err := json.Unmarshal(env.Data, &req); err != nil {
			return err
		}
		f.st.Heartbeat(req.NodeID, req.CapacityBytes, req.UsedBytes, time.Unix(req.AtUnix, 0).UTC())
		return nil

	default:
		return fmt.Errorf("unknown command type: %s", env.Type)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(f.st); err != nil {
		return nil, err
	}
	return &fsmSnapshot{b: buf.Bytes()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	type wrapper struct {
		Version int
		At      time.Time
		State   []byte
	}
	var w wrapper
	if err := gob.NewDecoder(rc).Decode(&w); err != nil {
		return err
	}
	var st State
	if err := gob.NewDecoder(bytes.NewReader(w.State)).Decode(&st); err != nil {
		return err
	}
	f.mu.Lock()
	f.st = &st
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct {
	b []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	// include a tiny header version to allow future migrations
	type wrapper struct {
		Version int
		At      time.Time
		State   []byte
	}
	w := wrapper{Version: 1, At: time.Now().UTC(), State: s.b}
	if err := gob.NewEncoder(sink).Encode(&w); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}

func (s *fsmSnapshot) Release() {}

