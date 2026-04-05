package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/pflag"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/pkg/client"
)

func main() {
	master := pflag.StringP("master", "m", "127.0.0.1:9090", "master gRPC address")
	pflag.Parse()

	args := pflag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: godfs-client [--master addr] <command> [args]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	c, err := client.New(*master)
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
	default:
		log.Fatalf("unknown command %q", args[0])
	}

	if err != nil {
		log.Fatal(err)
	}
}
