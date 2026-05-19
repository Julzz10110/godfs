package usecase

import (
	"errors"
	"testing"

	"godfs/internal/domain"
)

func TestNormalizeFSPath(t *testing.T) {
	t.Parallel()
	ok, err := NormalizeFSPath("/a/b")
	if err != nil || ok != "/a/b" {
		t.Fatalf("got %q err=%v", ok, err)
	}
	for _, p := range []string{"", "relative", "/"} {
		_, err := NormalizeFSPath(p)
		if !errors.Is(err, domain.ErrInvalidPath) {
			t.Fatalf("%q: got %v", p, err)
		}
	}
}

func TestNormalizeFSDirPath(t *testing.T) {
	t.Parallel()
	ok, err := NormalizeFSDirPath("/")
	if err != nil || ok != "/" {
		t.Fatalf("got %q err=%v", ok, err)
	}
	if _, err := NormalizeFSDirPath(""); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatal(err)
	}
}
