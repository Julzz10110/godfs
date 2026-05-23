//go:build linux

package main

import (
	"context"
	"sort"
)

type writeRange struct {
	off  int64
	data []byte
}

// fuseWriteBuffer accumulates writes until Flush (FUSE variant B).
type fuseWriteBuffer struct {
	ranges []writeRange
}

func (b *fuseWriteBuffer) reset() {
	b.ranges = b.ranges[:0]
}

func (b *fuseWriteBuffer) empty() bool {
	return len(b.ranges) == 0
}

func (b *fuseWriteBuffer) write(off int64, data []byte) {
	if len(data) == 0 {
		return
	}
	b.ranges = append(b.ranges, writeRange{off: off, data: append([]byte(nil), data...)})
}

func overlayBuffered(dest []byte, off int64, b *fuseWriteBuffer) {
	if b == nil || b.empty() {
		return
	}
	for i := range dest {
		one := dest[i : i+1]
		if n, ok := b.readAt(one, off+int64(i)); ok && n == 1 {
			dest[i] = one[0]
		}
	}
}

func (b *fuseWriteBuffer) readAt(dest []byte, off int64) (int, bool) {
	if len(dest) == 0 {
		return 0, true
	}
	end := off + int64(len(dest))
	var n int
	for _, r := range b.ranges {
		rEnd := r.off + int64(len(r.data))
		if rEnd <= off || r.off >= end {
			continue
		}
		lo := off
		if r.off > lo {
			lo = r.off
		}
		hi := end
		if rEnd < hi {
			hi = rEnd
		}
		srcOff := lo - r.off
		dstOff := lo - off
		copied := copy(dest[dstOff:dstOff+(hi-lo)], r.data[srcOff:])
		n += copied
	}
	return n, n == len(dest)
}

type writeAtFunc func(ctx context.Context, path string, off int64, data []byte) error

func (b *fuseWriteBuffer) flush(ctx context.Context, path string, writeAt writeAtFunc) error {
	if b.empty() {
		return nil
	}
	merged := mergeWriteRanges(b.ranges)
	for _, m := range merged {
		if err := writeAt(ctx, path, m.off, m.data); err != nil {
			return err
		}
	}
	b.reset()
	return nil
}

func mergeWriteRanges(in []writeRange) []writeRange {
	if len(in) == 0 {
		return nil
	}
	sorted := append([]writeRange(nil), in...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].off < sorted[j].off })
	var out []writeRange
	for _, r := range sorted {
		if len(out) == 0 {
			out = append(out, writeRange{off: r.off, data: append([]byte(nil), r.data...)})
			continue
		}
		last := &out[len(out)-1]
		lastEnd := last.off + int64(len(last.data))
		if r.off > lastEnd {
			out = append(out, writeRange{off: r.off, data: append([]byte(nil), r.data...)})
			continue
		}
		rEnd := r.off + int64(len(r.data))
		newEnd := lastEnd
		if rEnd > newEnd {
			newEnd = rEnd
		}
		buf := make([]byte, newEnd-last.off)
		copy(buf, last.data)
		copy(buf[r.off-last.off:], r.data)
		last.data = buf
	}
	return out
}
