package security

import "sync/atomic"

// AuthHolder holds the active Auth config; supports hot-swap when keys are loaded from @file paths.
type AuthHolder struct {
	p atomic.Pointer[Auth]
}

// NewAuthHolder wraps an initial Auth instance (may be disabled).
func NewAuthHolder(initial *Auth) *AuthHolder {
	h := &AuthHolder{}
	if initial != nil {
		h.p.Store(initial)
	}
	return h
}

// Current returns the active Auth (never nil after NewAuthHolder with non-nil initial).
func (h *AuthHolder) Current() *Auth {
	if h == nil {
		return &Auth{}
	}
	if a := h.p.Load(); a != nil {
		return a
	}
	return &Auth{}
}

// Store replaces the active Auth (no-op if next is nil).
func (h *AuthHolder) Store(next *Auth) {
	if h == nil || next == nil {
		return
	}
	h.p.Store(next)
}
