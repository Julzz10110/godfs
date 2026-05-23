package raftmeta

import (
	"fmt"
	"time"

	"godfs/internal/domain"
)

func raftChunkSlotsForSize(size, chunkSize int64) int {
	if size <= 0 || chunkSize <= 0 {
		return 0
	}
	return int((size-1)/chunkSize) + 1
}

func ensureRaftChunkSlots(chunks []domain.ChunkID, slots int) []domain.ChunkID {
	for len(chunks) < slots {
		chunks = append(chunks, "")
	}
	return chunks
}

// TruncateFile sets file size; shrinking returns chunks removed from metadata.
func (s *State) TruncateFile(fp string, size int64) ([]domain.ChunkDeleteInfo, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid size")
	}
	fr, ok := s.Files[fp]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if fr.DeletedAtUnix > 0 {
		return nil, domain.ErrNotFound
	}
	if fr.Size == size {
		return nil, nil
	}

	now := time.Now().UTC()
	slots := raftChunkSlotsForSize(size, s.ChunkSize)
	var infos []domain.ChunkDeleteInfo

	if size < fr.Size {
		for i := slots; i < len(fr.Chunks); i++ {
			cid := fr.Chunks[i]
			if cid == "" {
				continue
			}
			if info, ok := s.removeChunkLocked(cid); ok {
				infos = append(infos, info)
			}
		}
		fr.Chunks = fr.Chunks[:slots]
	} else {
		fr.Chunks = ensureRaftChunkSlots(fr.Chunks, slots)
	}

	fr.Size = size
	fr.Modified = now
	invalidateRaftTrailingChunkChecksum(s, fr.Chunks, size)
	return infos, nil
}

func invalidateRaftTrailingChunkChecksum(s *State, fileChunks []domain.ChunkID, fileSize int64) {
	if fileSize <= 0 || s.ChunkSize <= 0 || len(fileChunks) == 0 {
		return
	}
	tail := fileSize % s.ChunkSize
	if tail == 0 {
		return
	}
	cid := fileChunks[len(fileChunks)-1]
	if cid == "" {
		return
	}
	if cr, ok := s.Chunks[cid]; ok {
		cr.Checksum = nil
	}
}

func (s *State) removeChunkLocked(cid domain.ChunkID) (domain.ChunkDeleteInfo, bool) {
	cr, ok := s.Chunks[cid]
	if !ok {
		return domain.ChunkDeleteInfo{}, false
	}
	var addrs []string
	for _, r := range cr.Replicas {
		addrs = append(addrs, r.Address)
	}
	info := domain.ChunkDeleteInfo{ChunkID: cid, Replicas: addrs}
	if len(addrs) > 0 {
		set := s.PendingDeletes[cid]
		if set == nil {
			set = map[string]*pendingDelete{}
			s.PendingDeletes[cid] = set
		}
		for _, a := range addrs {
			if _, ok := set[a]; !ok {
				set[a] = &pendingDelete{CreatedUnix: time.Now().UTC().Unix()}
			}
		}
	}
	s.releaseChunkFromReplicas(cr.Replicas)
	delete(s.Chunks, cid)
	return info, true
}
