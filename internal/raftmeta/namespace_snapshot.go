package raftmeta

// NamespaceSnapshot returns best-effort namespace scale metrics from replicated metadata.
func (s *Service) NamespaceSnapshot() (files, dirs, chunks int, logicalBytes int64) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	st := s.fsm.st
	if st == nil {
		return 0, 0, 0, 0
	}
	files = len(st.Files)
	dirs = len(st.Dirs)
	chunks = len(st.Chunks)
	for _, f := range st.Files {
		if f != nil {
			logicalBytes += f.Size
		}
	}
	return files, dirs, chunks, logicalBytes
}
