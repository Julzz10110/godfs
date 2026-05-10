package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupRestore_RoundTrip(t *testing.T) {
	td := t.TempDir()
	dataDir := filepath.Join(td, "data")
	backupDir := filepath.Join(td, "backup")

	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		t.Fatal(err)
	}
	// create two chunks
	if err := os.WriteFile(filepath.Join(dataDir, "c1.chk"), []byte("hello"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "c2.chk"), []byte("world"), 0o640); err != nil {
		t.Fatal(err)
	}

	if err := runBackup(dataDir, backupDir, 2, true); err != nil {
		t.Fatalf("backup: %v", err)
	}

	restoredData := filepath.Join(td, "restored")
	if err := runRestore(restoredData, backupDir, 2, false, true); err != nil {
		t.Fatalf("restore: %v", err)
	}

	b1, err := os.ReadFile(filepath.Join(restoredData, "c1.chk"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b1) != "hello" {
		t.Fatalf("c1 mismatch: %q", string(b1))
	}
	b2, err := os.ReadFile(filepath.Join(restoredData, "c2.chk"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b2) != "world" {
		t.Fatalf("c2 mismatch: %q", string(b2))
	}
}

