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

func TestNormalizeStatPath(t *testing.T) {
	t.Parallel()
	ok, err := NormalizeStatPath("/")
	if err != nil || ok != "/" {
		t.Fatalf("got %q err=%v", ok, err)
	}
}

func TestNormalizeRenamePath(t *testing.T) {
	t.Parallel()
	if _, err := NormalizeRenamePath("/"); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("got %v", err)
	}
	ok, err := NormalizeRenamePath("/a")
	if err != nil || ok != "/a" {
		t.Fatalf("got %q err=%v", ok, err)
	}
}
