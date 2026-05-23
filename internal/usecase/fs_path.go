package usecase

import (
	"path"

	"godfs/internal/domain"
)

// NormalizeFSPath validates a file path (absolute, non-root) and returns its cleaned form.
func NormalizeFSPath(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", domain.ErrInvalidPath
	}
	c := path.Clean(p)
	if c == "/" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}

// NormalizeFSDirPath validates a directory path (absolute) and returns its cleaned form.
func NormalizeFSDirPath(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", domain.ErrInvalidPath
	}
	c := path.Clean(p)
	if c == "" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}

// NormalizeStatPath validates any namespace path for Stat (includes "/").
func NormalizeStatPath(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", domain.ErrInvalidPath
	}
	c := path.Clean(p)
	if c == "" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}

// NormalizeRenamePath validates old/new paths for Rename (root "/" is not allowed).
func NormalizeRenamePath(p string) (string, error) {
	c, err := NormalizeStatPath(p)
	if err != nil {
		return "", err
	}
	if c == "/" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}
