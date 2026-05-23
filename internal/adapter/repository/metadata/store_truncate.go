package metadata

import (
	"context"
	"fmt"
	"time"

	"godfs/internal/domain"
)

func chunkSlotsForSize(size, chunkSize int64) int {
	if size <= 0 || chunkSize <= 0 {
		return 0
	}
	return int((size-1)/chunkSize) + 1
}

func ensureChunkSlots(chunks []domain.ChunkID, slots int) []domain.ChunkID {
	for len(chunks) < slots {
		chunks = append(chunks, "")
	}
	return chunks
}

// TruncateFile sets file size; shrinking returns chunk locations removed from metadata.
func (s *Store) TruncateFile(_ context.Context, p string, size int64) ([]domain.ChunkDeleteInfo, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid size")
	}
	fp, err := normalizePath(p)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok || !s.fileVisibleLocked(fr, time.Now().UTC()) {
		return nil, domain.ErrNotFound
	}
	if fr.size == size {
		return nil, nil
	}

	now := time.Now().UTC()
	slots := chunkSlotsForSize(size, s.chunkSize)
	var infos []domain.ChunkDeleteInfo

	if size < fr.size {
		for i := slots; i < len(fr.chunks); i++ {
			cid := fr.chunks[i]
			if cid == "" {
				continue
			}
			if info, ok := s.removeChunkLocked(cid); ok {
				infos = append(infos, info)
			}
		}
		fr.chunks = fr.chunks[:slots]
	} else {
		fr.chunks = ensureChunkSlots(fr.chunks, slots)
	}

	fr.size = size
	fr.modified = now
	invalidateTrailingChunkChecksum(s.chunks, fr.chunks, size, s.chunkSize)
	return infos, nil
}

// invalidateTrailingChunkChecksum clears the stored checksum when the file ends
// inside a committed chunk (truncate shrink); the checksum still covers the full write on disk.
func invalidateTrailingChunkChecksum(chunks map[domain.ChunkID]*chunkRec, fileChunks []domain.ChunkID, fileSize, chunkSize int64) {
	if fileSize <= 0 || chunkSize <= 0 || len(fileChunks) == 0 {
		return
	}
	tail := fileSize % chunkSize
	if tail == 0 {
		return
	}
	cid := fileChunks[len(fileChunks)-1]
	if cid == "" {
		return
	}
	if cr, ok := chunks[cid]; ok {
		cr.checksum = nil
	}
}

func (s *Store) removeChunkLocked(cid domain.ChunkID) (domain.ChunkDeleteInfo, bool) {
	cr, ok := s.chunks[cid]
	if !ok {
		return domain.ChunkDeleteInfo{}, false
	}
	addrs := make([]string, len(cr.replicas))
	for i, r := range cr.replicas {
		addrs[i] = r.Address
	}
	info := domain.ChunkDeleteInfo{ChunkID: cid, Replicas: addrs}
	if len(addrs) > 0 {
		set := s.pendingDeletes[cid]
		if set == nil {
			set = map[string]*pendingChunkDelete{}
			s.pendingDeletes[cid] = set
		}
		for _, a := range addrs {
			if _, ok := set[a]; !ok {
				set[a] = &pendingChunkDelete{CreatedUnix: time.Now().UTC().Unix()}
			}
		}
	}
	s.releaseChunkFromReplicas(cr.replicas)
	delete(s.chunks, cid)
	return info, true
}
