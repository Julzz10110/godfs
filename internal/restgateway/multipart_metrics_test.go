package restgateway

import (
	"bytes"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMultipartMetrics_initPartAbort(t *testing.T) {
	dir := t.TempDir()
	m := newMultipartManager(dir)

	id, err := m.Init("/obj.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(multipartUploadsActive); got != 1 {
		t.Fatalf("active=%v want 1", got)
	}

	body := bytes.Repeat([]byte("x"), 100)
	_, _, err = m.PutPart(id, 1, strings.NewReader(string(body)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(multipartPartsStagedBytes); got != 100 {
		t.Fatalf("staged=%v want 100", got)
	}

	m.Abort(id)
	if got := testutil.ToFloat64(multipartUploadsActive); got != 0 {
		t.Fatalf("after abort active=%v", got)
	}
	if got := testutil.ToFloat64(multipartPartsStagedBytes); got != 0 {
		t.Fatalf("after abort staged=%v", got)
	}
}

func TestMultipartMetrics_replacePartAdjustsStagedBytes(t *testing.T) {
	dir := t.TempDir()
	m := newMultipartManager(dir)
	id, err := m.Init("/r.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.PutPart(id, 1, strings.NewReader("aaaa"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.PutPart(id, 1, strings.NewReader("bb"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(multipartPartsStagedBytes); got != 2 {
		t.Fatalf("staged=%v want 2 after replace", got)
	}
	m.Abort(id)
}
