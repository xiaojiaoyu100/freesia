package entry

import (
	"testing"
	"time"
)

func TestWithExpiration(t *testing.T) {
	ts := [...]struct {
		in  time.Duration
		out time.Duration
	}{
		{
			3 * time.Second,
			3 * time.Second,
		},
		{
			10 * time.Millisecond,
			10 * time.Millisecond,
		},
		{
			1 * time.Nanosecond,
			1 * time.Nanosecond,
		},
	}

	for _, s := range ts {
		e, err := New(
			WithKey("test"),
			WithExpiration(s.in))
		if err != nil {
			t.Errorf("new entry err: %q", err)
			continue
		}

		if int64(e.Exp()) != int64(s.out) {
			t.Errorf("got %q, want: %q", e.Exp(), s.out)
		}
	}

}
