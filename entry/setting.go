package entry

import (
	"time"
)

const (
	defaultLocalExpiration = 5 * time.Second
)

// DefaultLocalExpiration returns the default of local expiration.
func DefaultLocalExpiration() time.Duration {
	return defaultLocalExpiration
}

// Setting configures an entry.
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

// EnableLocalCache enables local cache.
func EnableLocalCache() Setting {
	return func(e *Entry) error {
		e.enableLocalCache = true
		e.localExp = defaultLocalExpiration
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
