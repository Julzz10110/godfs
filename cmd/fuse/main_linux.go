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
		cacheTTL   = flag.Duration("cache-ttl", 2*time.Second, "TTL for Stat() and Readdir() entries (0 disables caching)")
		negTTL     = flag.Duration("negcache-ttl", time.Second, "TTL for negative path lookups (ENOENT cache)")
		dirTTL     = flag.Duration("dircache-ttl", 0, "Readdir cache TTL (0 means same as -cache-ttl)")
		rpcTimeout = flag.Duration("rpc-timeout", 0, "optional deadline for each gRPC call from FUSE (0 = use context from kernel only)")
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

	var cache *metaPathCache
	if *cacheTTL > 0 {
		dTTL := *dirTTL
		if dTTL <= 0 {
			dTTL = *cacheTTL
		}
		cache = newMetaPathCache(*cacheTTL, *negTTL, dTTL)
	}

	root := &node{
		cli:         cli,
		cache:       cache,
		full:        mp,
		isDir:       true,
		mode:        0o755,
		rpcTimeout:  *rpcTimeout,
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
