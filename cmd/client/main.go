package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
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
		fmt.Fprintln(os.Stderr, "usage: godfs-client [--master addr] [--api-key key] <command> [args]")
		fmt.Fprintln(os.Stderr, "commands: mkdir create write read rm mv ls stat snapshot masters nodes rebalance-run")
		fmt.Fprintln(os.Stderr, "         chunks under-replicated [--json]  (admin; exit 1 if any under-replicated)")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	c, err := client.NewWithOptions(*master, 0, *apiKey)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

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
			log.Fatal("snapshot <create|list|get|delete|restore> ...")
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
		case "restore":
			if len(args) < 3 || len(args) > 4 {
				log.Fatal("snapshot restore <manifest.json> [--force]")
			}
			force := false
			if len(args) == 4 {
				if args[3] != "--force" {
					log.Fatal("snapshot restore <manifest.json> [--force]")
				}
				force = true
			}
			b, rerr := os.ReadFile(args[2])
			if rerr != nil {
				err = rerr
				break
			}
			var man godfsv1.BackupManifest
			if uerr := protojson.Unmarshal(b, &man); uerr != nil {
				err = uerr
				break
			}
			err = c.RestoreSnapshot(ctx, &man, force)
		default:
			log.Fatalf("unknown snapshot subcommand %q", args[1])
		}
	case "rebalance-run":
		steps := int32(1)
		if len(args) >= 2 && args[1] == "--steps" {
			if len(args) != 3 {
				log.Fatal("rebalance-run [--steps N]")
			}
			n, e := strconv.Atoi(args[2])
			if e != nil || n < 1 {
				log.Fatal("rebalance-run --steps N (N >= 1)")
			}
			steps = int32(n)
		} else if len(args) > 1 {
			log.Fatal("rebalance-run [--steps N]")
		}
		var ex int32
		ex, err = c.RunRebalanceNow(ctx, steps)
		if err == nil {
			fmt.Printf("executed=%d\n", ex)
		}
	case "nodes":
		var nodes []*godfsv1.ChunkNodeEntry
		nodes, err = c.ListChunkNodes(ctx)
		if err != nil {
			break
		}
		for _, n := range nodes {
			a := "dead"
			if n.GetAlive() {
				a = "alive"
			}
			fmt.Printf("%s\t%s\tcap=%d\tused=%d\tlast_seen=%d\t%s\n",
				n.GetNodeId(), n.GetGrpcAddress(), n.GetCapacityBytes(), n.GetUsedBytes(), n.GetLastSeenUnix(), a)
		}
	case "chunks":
		if len(args) < 2 || args[1] != "under-replicated" {
			log.Fatal("chunks under-replicated [--json]")
		}
		jsonOut := false
		switch len(args) {
		case 2:
		case 3:
			if args[2] != "--json" {
				log.Fatal("chunks under-replicated [--json]")
			}
			jsonOut = true
		default:
			log.Fatal("chunks under-replicated [--json]")
		}
		var entries []*godfsv1.UnderReplicatedChunkEntry
		var total int32
		entries, total, err = c.ListUnderReplicatedChunks(ctx, 0)
		if err != nil {
			break
		}
		if jsonOut {
			resp := &godfsv1.ListUnderReplicatedChunksResponse{Chunks: entries, TotalCount: total}
			b, merr := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(resp)
			if merr != nil {
				err = merr
				break
			}
			fmt.Println(string(b))
		} else {
			fmt.Printf("total_count=%d\n", total)
			for _, e := range entries {
				fmt.Printf("%s\talive=%d/%d\tdead_nodes=%v\tpaths=%v\n",
					e.GetChunkId(), e.GetAliveReplicas(), e.GetTargetReplication(),
					e.GetDeadNodeIds(), e.GetSamplePaths())
			}
		}
		if total > 0 {
			os.Exit(1)
		}
	case "masters":
		if len(args) < 2 {
			log.Fatal("masters <list|add|remove> ...")
		}
		switch args[1] {
		case "list":
			var peers []*godfsv1.MasterPeer
			var leader string
			peers, leader, err = c.ListMasters(ctx)
			if err != nil {
				break
			}
			fmt.Printf("leader_node_id=%s\n", leader)
			for _, p := range peers {
				role := "nonvoter"
				if p.GetVoter() {
					role = "voter"
				}
				fmt.Printf("%s\t%s\t%s\t%s\n", p.GetNodeId(), p.GetRaftAddress(), p.GetGrpcAddress(), role)
			}
		case "add":
			if len(args) != 5 {
				log.Fatal("masters add <node_id> <raft_addr> <grpc_addr>")
			}
			err = c.AddMaster(ctx, args[2], args[3], args[4])
		case "remove":
			if len(args) != 3 {
				log.Fatal("masters remove <node_id>")
			}
			err = c.RemoveMaster(ctx, args[2])
		default:
			log.Fatalf("unknown masters subcommand %q", args[1])
		}
	default:
		log.Fatalf("unknown command %q", args[0])
	}

	if err != nil {
		log.Fatal(err)
	}
}
