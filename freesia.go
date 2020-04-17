package freesia

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"github.com/xiaojiaoyu100/curlew"
	"github.com/xiaojiaoyu100/freesia/entry"
	"github.com/xiaojiaoyu100/lizard/mass"
	"github.com/xiaojiaoyu100/roc"
	"os"
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
	if e.EnableLocalExp() {
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
		if e.EnableLocalExp() {
			err := f.cache.Set(e.Key(), e.Data(), e.LocalExp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Get returns the cache.
func (f *Freesia) Get(e *entry.Entry) error {
	if e.EnableLocalExp() {
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
	b, err := f.store.Get(e.Key()).Bytes()
	switch err {
	case nil:
		err = e.Decode(b)
		if err != nil {
			return redis.Nil
		}
		e.SetSource(entry.SourceCenter)
		return nil
	default:
		return err
	}
}

// GetWithTTL gets value with ttl.
func (f *Freesia) GetWithTTL(e *entry.Entry) error {
	if e.EnableLocalExp() {
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
	pipe.TTL(e.Key())
	pipe.Get(e.Key())
	cmds, _ := pipe.Exec()
	ttl, _ := cmds[0].(*redis.DurationCmd).Result()
	e.SetTTL(ttl.Seconds())
	b, err := cmds[1].(*redis.StringCmd).Bytes()
	switch err {
	case nil:
		err = e.Decode(b)
		if err != nil {
			return redis.Nil
		}
		e.SetSource(entry.SourceCenter)
	default:
		return err
	}
	return nil
}

func (f *Freesia) batchGet(es ...*entry.Entry) ([]*entry.Entry, error) {
	pipe := f.store.Pipeline()
	found := make(map[*entry.Entry]struct{})
	ret := make(map[*redis.StringCmd]*entry.Entry)
	for _, e := range es {
		if e.EnableLocalExp() {
			b, err := f.cache.Get(e.Key())
			switch err {
			case roc.ErrMiss:
				cmd := pipe.Get(e.Key())
				ret[cmd] = e
			case nil:
				if data, ok := b.([]byte); ok {
					err = e.Decode(data)
					if err != nil {
						return nil, err
					}
					found[e] = struct{}{}
				}
			default:
				return nil, err

			}
		} else {
			cmd := pipe.Get(e.Key())
			ret[cmd] = e
		}
	}

	// all keys found in local store.
	if len(ret) == 0 {
		return []*entry.Entry{}, nil
	}

	cmders, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	for _, cmder := range cmders {
		cmd, ok := cmder.(*redis.StringCmd)
		if !ok {
			continue
		}
		e, ok := ret[cmd]
		if !ok {
			continue
		}
		b, err := cmd.Bytes()
		switch err {
		case redis.Nil:

		case nil:
			err = e.Decode(b)
			if err != nil {
				continue
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
