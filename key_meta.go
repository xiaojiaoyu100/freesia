package freesia

import "time"

type KeyMeta struct {
	EnableLocalCache bool
	Expiration       time.Duration
	SingleFlight     bool
}
