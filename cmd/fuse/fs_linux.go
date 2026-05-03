//go:build linux

package main

import (
	"context"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"godfs/pkg/client"
)

type node struct {
	fs.Inode

	cli        *client.Client
	cache      *metaPathCache
	rpcTimeout time.Duration

	full  string
	isDir bool
	mode  uint32
	size  int64
	mtime time.Time
	ctime time.Time
}

func (n *node) applyStat(st *client.FileInfo) {
	n.isDir = st.IsDir
	n.mode = st.Mode
	n.size = st.Size
	n.mtime = st.ModTime
	n.ctime = st.CreateAt
}

func (n *node) ensureMeta(ctx context.Context) syscall.Errno {
	if n.full == "/" {
		n.isDir = true
		n.mode = 0o755
		n.size = 0
		return 0
	}
	if n.cache != nil {
		if n.cache.getNeg(n.full) {
			return syscall.ENOENT
		}
		if info, ok := n.cache.getStat(n.full); ok {
			n.applyStat(&info)
			return 0
		}
	}
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	st, err := n.cli.Stat(cctx, n.full)
	if err != nil {
		errno := grpcToErrno(err)
		if errno == syscall.ENOENT && n.cache != nil {
			n.cache.putNeg(n.full)
		}
		return errno
	}
	n.applyStat(st)
	if n.cache != nil {
		n.cache.putStat(n.full, st)
	}
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

func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return syscall.EOPNOTSUPP
	}
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MODE|fuse.FATTR_KILL_SUIDGID) != 0 {
		return syscall.EPERM
	}
	return n.Getattr(ctx, f, out)
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return nil, syscall.ENOENT
	}
	full := path.Join(n.full, name)
	child := &node{cli: n.cli, cache: n.cache, full: full, rpcTimeout: n.rpcTimeout}
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
	if n.cache != nil {
		if ents, ok := n.cache.getDir(n.full); ok {
			return fs.NewListDirStream(ents), 0
		}
	}
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	entries, err := n.cli.List(cctx, n.full)
	if err != nil {
		return nil, grpcToErrno(err)
	}
	var out []fuse.DirEntry
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		out = append(out, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	if n.cache != nil {
		n.cache.putDir(n.full, out)
	}
	return fs.NewListDirStream(out), 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_TRUNC != 0 {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if errno := n.ensureMeta(ctx); errno != 0 {
		return nil, 0, errno
	}
	if n.isDir {
		return nil, 0, syscall.EISDIR
	}
	writeable := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	return &fileHandle{n: n, writeable: writeable}, 0, 0
}

func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return nil, nil, 0, syscall.EINVAL
	}
	if flags&syscall.O_TRUNC != 0 {
		return nil, nil, 0, syscall.EOPNOTSUPP
	}
	full := path.Join(n.full, name)
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	if err := n.cli.Create(cctx, full); err != nil {
		return nil, nil, 0, grpcToErrno(err)
	}
	if n.cache != nil {
		n.cache.invalidateMutation(full)
	}
	child := &node{cli: n.cli, cache: n.cache, full: full, rpcTimeout: n.rpcTimeout}
	if errno := child.ensureMeta(cctx); errno != 0 {
		child.isDir = false
		child.mode = mode & 0777
		child.size = 0
		now := time.Now()
		child.mtime = now
		child.ctime = now
	}
	stable := fs.StableAttr{}
	ch := n.NewInode(ctx, child, stable)
	out.Attr.Mode = child.attrMode()
	out.Attr.Size = uint64(child.size)
	out.Attr.Mtime = uint64(child.mtime.Unix())
	out.Attr.Ctime = uint64(child.ctime.Unix())
	out.Attr.Atime = out.Attr.Mtime
	fh := &fileHandle{n: child, writeable: true}
	return ch, fh, 0, 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return nil, syscall.EINVAL
	}
	full := path.Join(n.full, name)
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	if err := n.cli.Mkdir(cctx, full); err != nil {
		return nil, grpcToErrno(err)
	}
	if n.cache != nil {
		n.cache.invalidateMutation(full)
	}
	child := &node{cli: n.cli, cache: n.cache, full: full, rpcTimeout: n.rpcTimeout}
	if errno := child.ensureMeta(cctx); errno != 0 {
		child.isDir = true
		child.mode = mode & 0777
		child.size = 0
		now := time.Now()
		child.mtime = now
		child.ctime = now
	}
	stable := fs.StableAttr{}
	ch := n.NewInode(ctx, child, stable)
	out.Attr.Mode = child.attrMode()
	out.Attr.Size = 0
	out.Attr.Mtime = uint64(child.mtime.Unix())
	out.Attr.Ctime = uint64(child.ctime.Unix())
	out.Attr.Atime = out.Attr.Mtime
	return ch, 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return syscall.EINVAL
	}
	full := path.Join(n.full, name)
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	st, err := n.cli.Stat(cctx, full)
	if err != nil {
		return grpcToErrno(err)
	}
	if st.IsDir {
		return syscall.EISDIR
	}
	if err := n.cli.Delete(cctx, full); err != nil {
		return grpcToErrno(err)
	}
	if n.cache != nil {
		n.cache.invalidateMutation(full)
	}
	return 0
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if name == "." || name == ".." || strings.Contains(name, "/") || name == "" {
		return syscall.EINVAL
	}
	full := path.Join(n.full, name)
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	st, err := n.cli.Stat(cctx, full)
	if err != nil {
		return grpcToErrno(err)
	}
	if !st.IsDir {
		return syscall.ENOTDIR
	}
	ents, err := n.cli.List(cctx, full)
	if err != nil {
		return grpcToErrno(err)
	}
	if len(ents) > 0 {
		return syscall.ENOTEMPTY
	}
	if err := n.cli.Delete(cctx, full); err != nil {
		return grpcToErrno(err)
	}
	if n.cache != nil {
		n.cache.invalidateMutation(full)
	}
	return 0
}

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		return syscall.EINVAL
	}
	if newName == "" || strings.Contains(newName, "/") {
		return syscall.EINVAL
	}
	np := renameTargetNode(newParent)
	if np == nil || np.cli != n.cli {
		return syscall.EXDEV
	}
	oldFull := path.Join(n.full, name)
	newFull := path.Join(np.full, newName)
	cctx, cancel := n.opCtx(ctx)
	defer cancel()
	if err := n.cli.Rename(cctx, oldFull, newFull); err != nil {
		return grpcToErrno(err)
	}
	if n.cache != nil {
		n.cache.invalidateMutation(oldFull)
		n.cache.invalidateMutation(newFull)
	}
	return 0
}

func renameTargetNode(newParent fs.InodeEmbedder) *node {
	if newParent == nil {
		return nil
	}
	np, ok := newParent.(*node)
	if !ok {
		return nil
	}
	return np
}

type fileHandle struct {
	n         *node
	writeable bool
}

func (f *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off < 0 {
		return nil, syscall.EINVAL
	}
	if len(dest) == 0 {
		return fuse.ReadResultData(nil), 0
	}
	cctx, cancel := f.n.opCtx(ctx)
	defer cancel()
	data, err := f.n.cli.ReadRange(cctx, f.n.full, off, int64(len(dest)))
	if err != nil {
		return nil, grpcToErrno(err)
	}
	return fuse.ReadResultData(data), 0
}

func (f *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if !f.writeable {
		return 0, syscall.EBADF
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if len(data) == 0 {
		return 0, 0
	}
	cctx, cancel := f.n.opCtx(ctx)
	defer cancel()
	if err := f.n.cli.WriteAt(cctx, f.n.full, off, data); err != nil {
		return 0, grpcToErrno(err)
	}
	if f.n.cache != nil {
		f.n.cache.invalidateMutation(f.n.full)
	}
	return uint32(len(data)), 0
}

func (f *fileHandle) Flush(ctx context.Context) syscall.Errno {
	if f.n.cache != nil {
		f.n.cache.invalidatePath(f.n.full)
	}
	return 0
}

func (f *fileHandle) Release(ctx context.Context) syscall.Errno {
	if f.n.cache != nil {
		f.n.cache.invalidatePath(f.n.full)
	}
	return 0
}

func (f *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if f.n.cache != nil {
		f.n.cache.invalidatePath(f.n.full)
	}
	return 0
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

var (
	_ fs.NodeMkdirer    = (*node)(nil)
	_ fs.NodeCreater    = (*node)(nil)
	_ fs.NodeUnlinker   = (*node)(nil)
	_ fs.NodeRmdirer    = (*node)(nil)
	_ fs.NodeRenamer    = (*node)(nil)
	_ fs.NodeSetattrer  = (*node)(nil)
	_ fs.FileWriter     = (*fileHandle)(nil)
	_ fs.FileFlusher    = (*fileHandle)(nil)
	_ fs.FileReleaser   = (*fileHandle)(nil)
	_ fs.FileFsyncer    = (*fileHandle)(nil)
)
