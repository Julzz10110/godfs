//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"godfs/pkg/client"
)

func main() {
	var (
		master     = flag.String("master", envOr("GODFS_MASTER", "127.0.0.1:9090"), "master gRPC address")
		mountpoint = flag.String("mountpoint", "", "mount point (required)")
		prefix     = flag.String("prefix", "/", "path prefix in goDFS namespace to mount")
		apiKey     = flag.String("api-key", envOr("GODFS_CLIENT_API_KEY", ""), "Bearer token / API key (optional)")
		debug      = flag.Bool("debug", false, "enable FUSE debug logging")
	)
	flag.Parse()

	if *mountpoint == "" {
		log.Fatal("missing --mountpoint")
	}

	mp, err := cleanMountPrefix(*prefix)
	if err != nil {
		log.Fatalf("prefix: %v", err)
	}

	ctx := context.Background()
	cli, err := client.NewWithOptions(*master, 0, *apiKey)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer cli.Close()

	root := &node{
		cli:    cli,
		full:   mp,
		isDir:  true,
		mode:   0o755,
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: *debug,
			Name:  "godfs",
		},
	}
	server, err := fs.Mount(*mountpoint, root, opts)
	if err != nil {
		log.Fatalf("mount: %v", err)
	}
	log.Printf("mounted %s at %s (master=%s)", mp, *mountpoint, *master)
	server.Wait()

	_ = ctx
}

type node struct {
	fs.Inode

	cli   *client.Client
	full  string // absolute path in goDFS namespace
	isDir bool
	mode  uint32
	size  int64
	mtime time.Time
	ctime time.Time
}

func (n *node) ensureMeta(ctx context.Context) syscall.Errno {
	if n.full == "/" {
		n.isDir = true
		n.mode = 0o755
		n.size = 0
		return 0
	}
	st, err := n.cli.Stat(ctx, n.full)
	if err != nil {
		return toErrno(err)
	}
	n.isDir = st.IsDir
	n.mode = st.Mode
	n.size = st.Size
	n.mtime = st.ModTime
	n.ctime = st.CreateAt
	return 0
}

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if errno := n.ensureMeta(ctx); errno != 0 {
		return errno
	}
	mode := uint32(n.mode)
	if n.isDir {
		mode |= uint32(syscall.S_IFDIR)
	} else {
		mode |= uint32(syscall.S_IFREG)
	}
	out.Mode = mode
	out.Size = uint64(n.size)
	out.Mtime = uint64(n.mtime.Unix())
	out.Ctime = uint64(n.ctime.Unix())
	out.Atime = out.Mtime
	return 0
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	full := path.Join(n.full, name)
	child := &node{cli: n.cli, full: full}
	if errno := child.ensureMeta(ctx); errno != 0 {
		return nil, errno
	}
	stable := fs.StableAttr{}
	ch := n.NewInode(ctx, child, stable)
	out.Attr.Mode = child.attrMode()
	out.Attr.Size = uint64(child.size)
	out.Attr.Mtime = uint64(child.mtime.Unix())
	out.Attr.Ctime = uint64(child.ctime.Unix())
	out.Attr.Atime = out.Attr.Mtime
	return ch, 0
}

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if errno := n.ensureMeta(ctx); errno != 0 {
		return nil, errno
	}
	if !n.isDir {
		return nil, syscall.ENOTDIR
	}
	entries, err := n.cli.List(ctx, n.full)
	if err != nil {
		return nil, toErrno(err)
	}
	var out []fuse.DirEntry
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		out = append(out, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return fs.NewListDirStream(out), 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Read-only prototype.
	if flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)|uint32(syscall.O_TRUNC)|uint32(syscall.O_CREAT)) != 0 {
		return nil, 0, syscall.EPERM
	}
	if errno := n.ensureMeta(ctx); errno != 0 {
		return nil, 0, errno
	}
	if n.isDir {
		return nil, 0, syscall.EISDIR
	}
	return &fileHandle{n: n}, 0, 0
}

type fileHandle struct {
	n *node
}

func (f *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off < 0 {
		return nil, syscall.EINVAL
	}
	if len(dest) == 0 {
		return fuse.ReadResultData(nil), 0
	}
	data, err := f.n.cli.ReadRange(ctx, f.n.full, off, int64(len(dest)))
	if err != nil {
		return nil, toErrno(err)
	}
	return fuse.ReadResultData(data), 0
}

func (n *node) attrMode() uint32 {
	mode := uint32(n.mode)
	if n.isDir {
		mode |= uint32(syscall.S_IFDIR)
	} else {
		mode |= uint32(syscall.S_IFREG)
	}
	return mode
}

func envOr(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func cleanMountPrefix(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("prefix must be absolute")
	}
	cp := path.Clean(p)
	if cp == "." {
		cp = "/"
	}
	if cp == "" {
		cp = "/"
	}
	return cp, nil
}

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	// Best-effort mapping; most errors will come as gRPC status wrapped by client.
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return syscall.ENOENT
	case strings.Contains(msg, "permission"):
		return syscall.EACCES
	case strings.Contains(msg, "is directory"):
		return syscall.EISDIR
	case strings.Contains(msg, "not directory"):
		return syscall.ENOTDIR
	default:
		return syscall.EIO
	}
}

