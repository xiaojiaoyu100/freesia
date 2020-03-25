package entry

import (
	"time"
)

type Setting func(e *Entry) error

// WithKey sets the key.
func WithKey(key string) Setting {
	return func(e *Entry) error {
		e.key = key
		return nil
	}
}

// WithExpiration sets the expiration for the key.
func WithExpiration(d time.Duration) Setting {
	return func(e *Entry) error {
		e.exp = d
		return nil
	}
}

// WithLocalExpiration sets the local expiration for the key.
func WithLocalExpiration(d time.Duration) Setting {
	return func(e *Entry) error {
		e.enableLocalCache = true
		e.localExp = d
		return nil
	}
}

// WithValue sets the corresponding value.
func WithValue(value interface{}) Setting {
	return func(e *Entry) error {
		e.value = value
		return nil
	}
}
