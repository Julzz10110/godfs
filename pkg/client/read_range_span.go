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

// readRangeSpan reads [start, endExclusive) from path without calling Stat.
// Caller must ensure the span is within the file and the path is a file.
func (c *Client) readRangeSpan(ctx context.Context, path string, start, endExclusive int64) ([]byte, error) {
	if endExclusive <= start {
		return []byte{}, nil
	}
	total := endExclusive - start
	out := make([]byte, total)
	dstOff := int64(0)
	fileOff := start

	for fileOff < endExclusive {
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
		remain := endExclusive - fileOff
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

// StreamRangeToWriter copies [offset, offset+length) from path to w in segments of at most segment bytes.
// A single Stat runs at the start. If segment <= 0, DefaultStreamSegmentBytes is used (4 MiB).
// Length may extend past EOF; the copy stops at file size. Returns total bytes written.
func (c *Client) StreamRangeToWriter(ctx context.Context, path string, offset, length, segment int64, w io.Writer) (written int64, err error) {
	if length < 0 || offset < 0 {
		return 0, fmt.Errorf("invalid range")
	}
	if length == 0 {
		return 0, nil
	}
	if segment <= 0 {
		segment = 4 << 20 // default segment when gateway passes 0
	}

	st, err := c.Stat(ctx, path)
	if err != nil {
		return 0, err
	}
	if st.IsDir {
		return 0, fmt.Errorf("is directory")
	}
	if st.Size == 0 || offset >= st.Size {
		return 0, nil
	}

	end := offset + length
	if end > st.Size {
		end = st.Size
	}
	if end <= offset {
		return 0, nil
	}

	pos := offset
	for pos < end {
		n := end - pos
		if n > segment {
			n = segment
		}
		data, err := c.readRangeSpan(ctx, path, pos, pos+n)
		if err != nil {
			return written, err
		}
		if len(data) == 0 {
			break
		}
		nw, err := w.Write(data)
		written += int64(nw)
		if err != nil {
			return written, err
		}
		if nw != len(data) {
			return written, io.ErrShortWrite
		}
		pos += int64(len(data))
	}
	return written, nil
}
