package client

import (
	"testing"
)

func BenchmarkWriteSegmentSplit(b *testing.B) {
	cs := int64(256 * 1024)
	data := make([]byte, 10*cs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var segs []writeSeg
		var pos int64
		remain := int64(len(data))
		for remain > 0 {
			chunkOff := pos % cs
			maxInChunk := cs - chunkOff
			n := remain
			if n > maxInChunk {
				n = maxInChunk
			}
			segs = append(segs, writeSeg{pos: pos, n: n})
			pos += n
			remain -= n
		}
		if len(segs) != 10 {
			b.Fatal(len(segs))
		}
		_ = segs
	}
}

func BenchmarkWriteSegmentSplit_64MiBChunks(b *testing.B) {
	cs := int64(64 * 1024 * 1024)
	data := make([]byte, 3*cs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var segs []writeSeg
		var pos int64
		remain := int64(len(data))
		for remain > 0 {
			chunkOff := pos % cs
			maxInChunk := cs - chunkOff
			n := remain
			if n > maxInChunk {
				n = maxInChunk
			}
			segs = append(segs, writeSeg{pos: pos, n: n})
			pos += n
			remain -= n
		}
		if len(segs) != 3 {
			b.Fatal(len(segs))
		}
		_ = segs
	}
}
