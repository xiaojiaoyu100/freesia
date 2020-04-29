package freesia

import (
	"context"
	"errors"
	"os"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"github.com/xiaojiaoyu100/curlew"
	"github.com/xiaojiaoyu100/freesia/entry"
	"github.com/xiaojiaoyu100/lizard/mass"
	"github.com/xiaojiaoyu100/roc"
)

// Freesia 多级缓存
type Freesia struct {
	store      Store
	cache      *roc.Cache
	dispatcher *curlew.Dispatcher
	pubSub     *redis.PubSub
	logger     *logrus.Logger
}

// New 生成一个实例
func New(store Store, setters ...Setter) (*Freesia, error) {
	var err error

	f := new(Freesia)
	f.store = store

	cache, err := roc.New()
	if err != nil {
		return nil, err
	}
	f.cache = cache

	monitor := func(err error) {}
	f.dispatcher, err = curlew.New(curlew.WithMonitor(monitor))
	if err != nil {
		return nil, err
	}

	f.pubSub = f.store.Subscribe(channel)

	f.sub()

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	f.logger = logger

	for _, setter := range setters {
		err = setter(f)
		if err != nil {
			return nil, err
		}
	}

	return f, nil
}

// Set sets a key-value.
func (f *Freesia) Set(e *entry.Entry) error {
	if err := e.Encode(); err != nil {
		return err
	}
	if err := f.store.Set(e.Key(), e.Data(), e.Exp()).Err(); err != nil {
		return err
	}
	if e.EnableLocalCache() {
		if err := f.cache.Set(e.Key(), e.Data(), e.LocalExp()); err != nil {
			return err
		}
	}
	return nil
}

// MSet batch sets entries.
func (f *Freesia) MSet(es ...*entry.Entry) error {
	pipe := f.store.Pipeline()
	for _, e := range es {
		if err := e.Encode(); err != nil {
			return err
		}
		pipe.Set(e.Key(), e.Data(), e.Exp())
	}
	_, err := pipe.Exec()
	if err != nil {
		return err
	}
	for _, e := range es {
		if e.EnableLocalCache() {
			err := f.cache.Set(e.Key(), e.Data(), e.LocalExp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Get gets value.
func (f *Freesia) Get(e *entry.Entry) error {
	if e.EnableLocalCache() {
		data, err := f.cache.Get(e.Key())
		switch err {
		case roc.ErrMiss:
		case nil:
			b, ok := data.([]byte)
			if err = e.Decode(b); ok && err != nil {
				return err
			}
			e.SetSource(entry.SourceLocal)
			return nil
		default:
			return err
		}
	}
	pipe := f.store.Pipeline()
	ttlCmd := pipe.TTL(e.Key())
	getCmd := pipe.Get(e.Key())
	_, err := pipe.Exec()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	b, err := getCmd.Bytes()
	switch err {
	case nil:
		{
			err = e.Decode(b)
			if err != nil {
				e.Reset()
				return redis.Nil
			}
			ttl, _ := ttlCmd.Result()
			e.SetTTL(ttl.Seconds())
			e.SetSource(entry.SourceCenter)
			if e.EnableLocalCache() && e.TTL() > entry.DefaultLocalExpiration().Seconds() {
				_ = f.cache.Set(e.Key(), e.Data(), entry.DefaultLocalExpiration())
			}
		}
	case redis.Nil:
		e.Reset()
		return redis.Nil
	default:
		return err
	}
	return nil
}

func (f *Freesia) batchGet(es ...*entry.Entry) ([]*entry.Entry, error) {
	pipe := f.store.Pipeline()
	found := make(map[*entry.Entry]struct{})

	type EntryCmd struct {
		*redis.StringCmd
		*redis.DurationCmd
	}

	ret := make(map[*entry.Entry]*EntryCmd)
	for _, e := range es {
		if e.EnableLocalCache() {
			b, err := f.cache.Get(e.Key())
			switch {
			case errors.Is(err, roc.ErrMiss):
				ttlCmd := pipe.TTL(e.Key())
				getCmd := pipe.Get(e.Key())
				ret[e] = &EntryCmd{
					getCmd,
					ttlCmd,
				}
			case err == nil:
				data, ok := b.([]byte)
				if err = e.Decode(data); ok && err != nil {
					return nil, err
				}
				e.SetSource(entry.SourceLocal)
				found[e] = struct{}{}
			default:
				return nil, err
			}
		} else {
			ttlCmd := pipe.TTL(e.Key())
			getCmd := pipe.Get(e.Key())
			ret[e] = &EntryCmd{
				getCmd,
				ttlCmd,
			}
		}
	}

	// all keys found in local store.
	if len(ret) == 0 {
		return []*entry.Entry{}, nil
	}

	_, err := pipe.Exec()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for _, e := range es {
		entryCmd, ok := ret[e]
		if !ok {
			continue
		}
		b, err := entryCmd.StringCmd.Bytes()
		switch {
		case errors.Is(err, redis.Nil):
			e.Reset()
		case err == nil:
			err = e.Decode(b)
			if err != nil {
				e.Reset()
				continue
			}
			ttl, _ := entryCmd.DurationCmd.Result()
			e.SetTTL(ttl.Seconds())
			e.SetSource(entry.SourceCenter)
			if e.EnableLocalCache() && e.TTL() > entry.DefaultLocalExpiration().Seconds() {
				_ = f.cache.Set(e.Key(), e.Data(), entry.DefaultLocalExpiration())
			}
			found[e] = struct{}{}
		default:
			return nil, err
		}
	}

	missEntries := make([]*entry.Entry, 0, len(es))
	for _, e := range es {
		_, ok := found[e]
		if !ok {
			missEntries = append(missEntries, e)
		}
	}
	return missEntries, nil
}

// MGet batch get entries.
func (f *Freesia) MGet(es ...*entry.Entry) ([]*entry.Entry, error) {
	batch := mass.New(len(es), 3000)
	missEntries := make([]*entry.Entry, 0, len(es))
	var start, length int
	for batch.Iter(&start, &length) {
		ee, err := f.batchGet(es[start : start+length]...)
		if err != nil {
			return nil, err
		}
		missEntries = append(missEntries, ee...)
	}
	return missEntries, nil
}

// Del deletes keys.
func (f *Freesia) Del(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := f.store.Del(keys...).Result()
	if err != nil {
		return err
	}
	if err := f.pub(keys...); err != nil {
		return err
	}
	return nil
}

func (f *Freesia) pub(keys ...string) error {
	b, err := msgpack.Marshal(keys)
	if err != nil {
		return err
	}
	_, err = f.store.Publish(channel, string(b)).Result()
	if err != nil {
		return err
	}
	return nil
}

func (f *Freesia) sub() {
	go func() {
		for message := range f.pubSub.Channel() {
			job := curlew.NewJob()
			job.Arg = message
			job.Fn = func(ctx context.Context, arg interface{}) error {
				message := arg.(*redis.Message)
				var keys []string
				if err := msgpack.Unmarshal([]byte(message.Payload), &keys); err != nil {
					return err
				}
				for _, key := range keys {
					f.logger.Printf("delete key: %s", key)
					if err := f.cache.Del(key); err != nil {
						f.logger.Errorf("async delete cache key = %s, err = %#v", key, err)
					}
				}
				return nil
			}
			f.dispatcher.Submit(job)
		}
	}()

}
