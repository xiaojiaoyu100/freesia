package freesia

import (
	"time"

	"github.com/go-redis/redis/v7"
)

// Store represents redis op.
type Store interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	MSet(pairs ...interface{}) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	MGet(keys ...string) *redis.SliceCmd
	Del(keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner
	Subscribe(channels ...string) *redis.PubSub
	Publish(channel string, message interface{}) *redis.IntCmd
}
