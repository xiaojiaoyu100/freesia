package freesia

import (
	"github.com/go-redis/redis"
	"time"
)

type Store interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
	Subscribe(channels ...string) *redis.PubSub
}
