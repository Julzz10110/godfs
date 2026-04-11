package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/protojson"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/pkg/client"
)

func main() {
	master := pflag.StringP("master", "m", "127.0.0.1:9090", "master gRPC address")
	apiKey := pflag.String("api-key", "", "Bearer token / API key (overrides GODFS_CLIENT_API_KEY)")
	pflag.Parse()

	args := pflag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: godfs-client [--master addr] <command> [args]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	c, err := client.NewWithOptions(*master, 0, *apiKey)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	switch args[0] {
	case "mkdir":
		if len(args) != 2 {
			log.Fatal("mkdir <path>")
		}
		err = c.Mkdir(ctx, args[1])
	case "create":
		if len(args) != 2 {
			log.Fatal("create <path>")
		}
		err = c.Create(ctx, args[1])
	case "write":
		if len(args) != 3 {
			log.Fatal("write <path> <local-file>")
		}
		data, e := os.ReadFile(args[2])
		if e != nil {
			log.Fatal(e)
		}
		err = c.Write(ctx, args[1], data)
	case "read":
		if len(args) != 3 {
			log.Fatal("read <path> <local-file>")
		}
		var data []byte
		data, err = c.Read(ctx, args[1])
		if err != nil {
			break
		}
		err = os.WriteFile(args[2], data, 0o644)
	case "rm":
		if len(args) != 2 {
			log.Fatal("rm <path>")
		}
		err = c.Delete(ctx, args[1])
	case "mv":
		if len(args) != 3 {
			log.Fatal("mv <old> <new>")
		}
		err = c.Rename(ctx, args[1], args[2])
	case "ls":
		if len(args) != 2 {
			log.Fatal("ls <path>")
		}
		var entries []*godfsv1.DirEntry
		entries, err = c.List(ctx, args[1])
		if err != nil {
			break
		}
		for _, e := range entries {
			kind := "f"
			if e.IsDir {
				kind = "d"
			}
			fmt.Printf("[%s] %s  %d\n", kind, e.Name, e.Size)
		}
	case "stat":
		if len(args) != 2 {
			log.Fatal("stat <path>")
		}
		var st *client.FileInfo
		st, err = c.Stat(ctx, args[1])
		if err != nil {
			break
		}
		fmt.Printf("is_dir=%v size=%d mode=%o mod=%s\n", st.IsDir, st.Size, st.Mode, st.ModTime)
	case "snapshot":
		if len(args) < 2 {
			log.Fatal("snapshot <create|list|get|delete> ...")
		}
		switch args[1] {
		case "create":
			if len(args) != 3 {
				log.Fatal("snapshot create <label>")
			}
			var id string
			var ts int64
			id, ts, err = c.CreateSnapshot(ctx, args[2])
			if err != nil {
				break
			}
			fmt.Printf("snapshot_id=%s created_at_unix=%d\n", id, ts)
		case "list":
			var entries []*godfsv1.SnapshotListEntry
			entries, err = c.ListSnapshots(ctx)
			if err != nil {
				break
			}
			for _, e := range entries {
				fmt.Printf("%s\t%s\t%d\tfiles=%d\n", e.GetSnapshotId(), e.GetLabel(), e.GetCreatedAtUnix(), e.GetFileCount())
			}
		case "get":
			if len(args) < 3 || len(args) > 4 {
				log.Fatal("snapshot get <snapshot_id> [manifest.json]")
			}
			var man *godfsv1.BackupManifest
			man, err = c.GetSnapshot(ctx, args[2])
			if err != nil {
				break
			}
			if len(args) == 4 {
				var b []byte
				b, err = protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(man)
				if err != nil {
					break
				}
				err = os.WriteFile(args[3], b, 0o644)
				if err != nil {
					break
				}
			} else {
				fmt.Printf("snapshot_id=%s label=%s created_at_unix=%d files=%d chunk_size=%d replication=%d\n",
					man.GetSnapshotId(), man.GetLabel(), man.GetCreatedAtUnix(), len(man.GetFiles()), man.GetChunkSizeBytes(), man.GetReplicationFactor())
				for _, f := range man.GetFiles() {
					fmt.Printf("  %s size=%d chunks=%d\n", f.GetPath(), f.GetSize(), len(f.GetChunks()))
				}
			}
		case "delete":
			if len(args) != 3 {
				log.Fatal("snapshot delete <snapshot_id>")
			}
			err = c.DeleteSnapshot(ctx, args[2])
		default:
			log.Fatalf("unknown snapshot subcommand %q", args[1])
		}
	default:
		log.Fatalf("unknown command %q", args[0])
	}

	if err != nil {
		log.Fatal(err)
	}
}
