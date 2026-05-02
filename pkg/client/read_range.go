package client

import (
	"context"
	"fmt"
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
	if end <= offset {
		return []byte{}, nil
	}

	return c.readRangeSpan(ctx, path, offset, end)
}
