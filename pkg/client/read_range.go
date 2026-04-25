package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// ReadRange reads up to length bytes from path starting at offset.
// It returns an empty slice when length == 0.
func (c *Client) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	if length < 0 || offset < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	if length == 0 {
		return []byte{}, nil
	}

	st, err := c.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if st.IsDir {
		return nil, fmt.Errorf("is directory")
	}
	if st.Size == 0 || offset >= st.Size {
		return []byte{}, nil
	}

	end := offset + length
	if end > st.Size {
		end = st.Size
	}
	total := end - offset
	if total <= 0 {
		return []byte{}, nil
	}

	out := make([]byte, total)
	dstOff := int64(0)
	fileOff := offset

	for fileOff < end {
		var gr *godfsv1.GetChunkForReadResponse
		err := grpcRetry(ctx, 5, func() error {
			var e error
			gr, e = c.master.GetChunkForRead(ctx, &godfsv1.GetChunkForReadRequest{
				Path:   path,
				Offset: fileOff,
			})
			return e
		})
		if err != nil {
			return nil, err
		}
		remain := end - fileOff
		want := gr.AvailableInChunk
		if want > remain {
			want = remain
		}
		if want <= 0 {
			break
		}

		var reps []string
		if len(gr.ReplicaLocations) > 0 {
			for _, loc := range gr.ReplicaLocations {
				reps = append(reps, loc.GrpcAddress)
			}
		} else {
			reps = gr.ReplicaAddresses
		}
		if len(reps) == 0 {
			return nil, fmt.Errorf("no replicas")
		}

		var readErr error
		for _, rep := range reps {
			cc, err := c.chunkConn(rep)
			if err != nil {
				readErr = err
				continue
			}
			ch := godfsv1.NewChunkServiceClient(cc)

			var rc godfsv1.ChunkService_ReadChunkClient
			err = grpcRetry(ctx, 5, func() error {
				var e error
				rc, e = ch.ReadChunk(ctx, &godfsv1.ReadChunkRequest{
					ChunkId:       gr.ChunkId,
					OffsetInChunk: gr.ChunkOffset,
					Length:        want,
				})
				return e
			})
			if err != nil {
				readErr = err
				continue
			}

			var streamErr error
			var verifyHash hash.Hash
			// Only verify checksum when reading the full "available" prefix from offset 0.
			// Partial reads starting at offset 0 (e.g. HTTP Range bytes=0-0) can't be validated.
			if len(gr.ChunkChecksumSha256) == 32 && gr.ChunkOffset == 0 && want == gr.AvailableInChunk {
				verifyHash = sha256.New()
			}
			var wrote int64
			for {
				msg, err := rc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					streamErr = err
					break
				}
				if verifyHash != nil {
					verifyHash.Write(msg.Data)
				}
				n := copy(out[dstOff+wrote:], msg.Data)
				wrote += int64(n)
				if n < len(msg.Data) {
					streamErr = fmt.Errorf("short buffer")
					break
				}
				if wrote >= want {
					break
				}
			}
			if streamErr == nil && wrote != want {
				streamErr = fmt.Errorf("short read")
			}
			if streamErr == nil && verifyHash != nil {
				if !bytes.Equal(verifyHash.Sum(nil), gr.ChunkChecksumSha256) {
					streamErr = fmt.Errorf("chunk checksum mismatch")
				}
			}
			if streamErr == nil {
				readErr = nil
				break
			}
			readErr = streamErr
		}
		if readErr != nil {
			return nil, readErr
		}

		dstOff += want
		fileOff += want
	}
	return out, nil
}

