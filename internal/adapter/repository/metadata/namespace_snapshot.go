package metadata

// NamespaceSnapshot returns best-effort namespace scale metrics from in-memory metadata.
func (s *Store) NamespaceSnapshot() (files, dirs, chunks int, logicalBytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files = len(s.files)
	dirs = len(s.dirs)
	chunks = len(s.chunks)
	for _, f := range s.files {
		if f != nil {
			logicalBytes += f.size
		}
	}
	return files, dirs, chunks, logicalBytes
}
