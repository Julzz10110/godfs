package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
)

type chunkIndexEntry struct {
	ChunkID          string `json:"chunk_id"`
	SizeBytes        int64  `json:"size_bytes"`
	ChecksumSHA256Hex string `json:"checksum_sha256_hex"`
	ModUnix          int64  `json:"mod_unix"`
}

func main() {
	if len(os.Args) < 2 {
		usageAndExit()
	}
	switch os.Args[1] {
	case "backup":
		backupCmd(os.Args[2:])
	case "restore":
		restoreCmd(os.Args[2:])
	default:
		usageAndExit()
	}
}

func usageAndExit() {
	fmt.Fprintln(os.Stderr, "usage: chunkctl <backup|restore> [flags]")
	os.Exit(2)
}

func backupCmd(args []string) {
	fs := pflag.NewFlagSet("backup", pflag.ExitOnError)
	dataDir := fs.String("data-dir", "./chunkdata", "ChunkServer data directory")
	outRoot := fs.String("out", "", "Output backup directory")
	concurrency := fs.Int("concurrency", max(1, runtime.GOMAXPROCS(0)), "Concurrent file copy workers")
	verify := fs.Bool("verify", true, "Verify SHA-256 after copy")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "flags: %v\n", err)
		os.Exit(2)
	}

	if strings.TrimSpace(*outRoot) == "" {
		fmt.Fprintln(os.Stderr, "--out is required")
		os.Exit(2)
	}

	if err := runBackup(*dataDir, *outRoot, *concurrency, *verify); err != nil {
		fmt.Fprintf(os.Stderr, "backup error: %v\n", err)
		os.Exit(1)
	}
}

func restoreCmd(args []string) {
	fs := pflag.NewFlagSet("restore", pflag.ExitOnError)
	dataDir := fs.String("data-dir", "./chunkdata", "ChunkServer data directory")
	inRoot := fs.String("in", "", "Input backup directory")
	concurrency := fs.Int("concurrency", max(1, runtime.GOMAXPROCS(0)), "Concurrent restore workers")
	overwrite := fs.Bool("overwrite", false, "Overwrite existing chunk files in data-dir")
	verify := fs.Bool("verify", true, "Verify SHA-256 after restore")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "flags: %v\n", err)
		os.Exit(2)
	}

	if strings.TrimSpace(*inRoot) == "" {
		fmt.Fprintln(os.Stderr, "--in is required")
		os.Exit(2)
	}
	if err := runRestore(*dataDir, *inRoot, *concurrency, *overwrite, *verify); err != nil {
		fmt.Fprintf(os.Stderr, "restore error: %v\n", err)
		os.Exit(1)
	}
}

func runBackup(dataDir, outRoot string, concurrency int, verify bool) error {
	dataDirAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	outRootAbs, err := filepath.Abs(outRoot)
	if err != nil {
		return err
	}

	chunksDir := filepath.Join(outRootAbs, "chunks")
	if err := os.MkdirAll(chunksDir, 0o750); err != nil {
		return err
	}
	indexPath := filepath.Join(outRootAbs, "index.jsonl")
	indexTmp := indexPath + ".tmp"

	chunkIDs, err := listChunkIDs(dataDirAbs)
	if err != nil {
		return err
	}
	sort.Strings(chunkIDs)

	// Copy chunks in parallel and collect index entries.
	type result struct {
		e   chunkIndexEntry
		err error
	}
	in := make(chan string)
	out := make(chan result)

	var wg sync.WaitGroup
	workers := max(1, concurrency)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range in {
				src := filepath.Join(dataDirAbs, id+".chk")
				dst := filepath.Join(chunksDir, id+".chk")
				e, err := copyChunkFile(src, dst, verify)
				out <- result{e: e, err: err}
			}
		}()
	}

	go func() {
		for _, id := range chunkIDs {
			in <- id
		}
		close(in)
		wg.Wait()
		close(out)
	}()

	entries := make([]chunkIndexEntry, 0, len(chunkIDs))
	for r := range out {
		if r.err != nil {
			return r.err
		}
		entries = append(entries, r.e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ChunkID < entries[j].ChunkID })

	f, err := os.OpenFile(indexTmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(bw)
	for i := range entries {
		if err := enc.Encode(&entries[i]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return atomicRename(indexTmp, indexPath)
}

func runRestore(dataDir, inRoot string, concurrency int, overwrite, verify bool) error {
	dataDirAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	inRootAbs, err := filepath.Abs(inRoot)
	if err != nil {
		return err
	}
	chunksDir := filepath.Join(inRootAbs, "chunks")
	indexPath := filepath.Join(inRootAbs, "index.jsonl")

	if err := os.MkdirAll(dataDirAbs, 0o750); err != nil {
		return err
	}

	entries, err := readIndex(indexPath)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("index is empty: %s", indexPath)
	}

	type job struct {
		e chunkIndexEntry
	}
	type result struct {
		err error
	}
	in := make(chan job)
	out := make(chan result)

	var wg sync.WaitGroup
	workers := max(1, concurrency)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range in {
				id := j.e.ChunkID
				src := filepath.Join(chunksDir, id+".chk")
				dst := filepath.Join(dataDirAbs, id+".chk")
				if !overwrite {
					if _, err := os.Stat(dst); err == nil {
						out <- result{err: fmt.Errorf("refusing to overwrite existing chunk: %s (use --overwrite)", dst)}
						continue
					}
				}
				if err := restoreOne(src, dst, j.e, verify); err != nil {
					out <- result{err: err}
					continue
				}
				out <- result{err: nil}
			}
		}()
	}

	go func() {
		for _, e := range entries {
			in <- job{e: e}
		}
		close(in)
		wg.Wait()
		close(out)
	}()

	for r := range out {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func listChunkIDs(dataDirAbs string) ([]string, error) {
	entries, err := os.ReadDir(dataDirAbs)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".chk") {
			continue
		}
		out = append(out, strings.TrimSuffix(name, ".chk"))
	}
	return out, nil
}

func copyChunkFile(src, dst string, verify bool) (chunkIndexEntry, error) {
	fi, err := os.Stat(src)
	if err != nil {
		return chunkIndexEntry{}, err
	}
	sumHex, size, err := sha256File(src)
	if err != nil {
		return chunkIndexEntry{}, err
	}
	if err := copyFileAtomic(src, dst); err != nil {
		return chunkIndexEntry{}, err
	}
	if verify {
		sum2, size2, err := sha256File(dst)
		if err != nil {
			return chunkIndexEntry{}, err
		}
		if size2 != size || sum2 != sumHex {
			return chunkIndexEntry{}, fmt.Errorf("copy verification failed for %s", filepath.Base(src))
		}
	}
	id := strings.TrimSuffix(filepath.Base(src), ".chk")
	return chunkIndexEntry{
		ChunkID:           id,
		SizeBytes:         size,
		ChecksumSHA256Hex: sumHex,
		ModUnix:           fi.ModTime().UTC().Unix(),
	}, nil
}

func restoreOne(src, dst string, idx chunkIndexEntry, verify bool) error {
	if err := copyFileAtomic(src, dst); err != nil {
		return err
	}
	if verify {
		sumHex, size, err := sha256File(dst)
		if err != nil {
			return err
		}
		if idx.SizeBytes > 0 && size != idx.SizeBytes {
			return fmt.Errorf("size mismatch for %s: got=%d want=%d", idx.ChunkID, size, idx.SizeBytes)
		}
		if idx.ChecksumSHA256Hex != "" && sumHex != idx.ChecksumSHA256Hex {
			return fmt.Errorf("checksum mismatch for %s: got=%s want=%s", idx.ChunkID, sumHex, idx.ChecksumSHA256Hex)
		}
	}
	return nil
}

func readIndex(indexPath string) ([]chunkIndexEntry, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []chunkIndexEntry
	sc := bufio.NewScanner(f)
	// allow long lines just in case (still bounded)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		var e chunkIndexEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("invalid index line: %w", err)
		}
		if e.ChunkID == "" {
			return nil, fmt.Errorf("invalid index entry: missing chunk_id")
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func sha256File(p string) (sumHex string, size int64, err error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func copyFileAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp := dst + fmt.Sprintf(".%d.tmp", time.Now().UTC().UnixNano())

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	_, cpErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if cpErr != nil {
		_ = os.Remove(tmp)
		return cpErr
	}
	if syncErr != nil {
		_ = os.Remove(tmp)
		return syncErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return atomicRename(tmp, dst)
}

func atomicRename(tmp, dst string) error {
	if err := os.Rename(tmp, dst); err != nil {
		// Windows cannot rename over an existing file.
		var pe *os.PathError
		if errors.As(err, &pe) {
			_ = os.Remove(dst)
			if err2 := os.Rename(tmp, dst); err2 == nil {
				return nil
			}
		}
		// best-effort cleanup
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
