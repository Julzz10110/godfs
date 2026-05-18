package raftmeta

import (
	"testing"
	"time"
)

func TestStateSoftDelete_Restore(t *testing.T) {
	st := NewState(1024, 1, time.Minute, 0)
	st.SoftDeleteGrace = time.Hour
	st.Dirs["/"] = struct{}{}
	st.Files["/t/f"] = &fileRec{DeletedAtUnix: time.Now().Unix()}
	if err := st.RestoreFile("/t/f", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if st.Files["/t/f"].DeletedAtUnix != 0 {
		t.Fatal("expected restored")
	}
}
