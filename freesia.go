package freesia

import (
	"context"
	"fmt"

	"github.com/xiaojiaoyu100/lizard/mass"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
	"github.com/xiaojiaoyu100/curlew"
	"github.com/xiaojiaoyu100/freesia/entry"
	"github.com/xiaojiaoyu100/roc"
)

type Freesia struct {
	store      Store
	cache      *roc.Cache
	dispatcher *curlew.Dispatcher
	pubsub     *redis.PubSub
}

func New(store Store, setters ...Setter) (*Freesia, error) {
	var err error

	f := new(Freesia)
	f.store = store
	for _, setter := range setters {
		err = setter(f)
		if err != nil {
			return nil, err
		}
	}

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

	f.pubsub = f.store.Subscribe(channel)

	f.sub()

	return f, nil
}

func (f *Freesia) Set(e *entry.Entry) error {
	if err := e.Encode(); err != nil {
		return errors.Wrapf(err, "encode key = %s, value = %+v", e.Key, e.Value)
	}
	if err := f.store.Set(e.Key, e.Data(), e.Exp()).Err(); err != nil {
		return errors.Wrapf(err, "store set key = %s, value = %+v", e.Key, e.Value)
	}
	if e.EnableLocalCache() {
		if err := f.cache.Set(e.Key, e.Data(), e.LocalExp()); err != nil {
			return errors.Wrapf(err, "cache set key = %s, value = %+v", e.Key, e.Value)
		}
	}
	return nil
}

func (f *Freesia) MSet(es ...*entry.Entry) error {
	pipe := f.store.Pipeline()
	for _, e := range es {
		if err := e.Encode(); err != nil {
			return errors.Wrapf(err, "encode key = %s, value = %+v", e.Key, e.Value)
		}
		pipe.Set(e.Key, e.Data(), e.Exp())
	}
	_, err := pipe.Exec()
	if err != nil {
		return errors.Wrapf(err, "pipeline exec")
	}
	for _, e := range es {
		if e.EnableLocalCache() {
			err := f.cache.Set(e.Key, e.Data(), e.LocalExp())
			if err != nil {
				return errors.Wrapf(err, "cache set key = %s, value = %+v", e.Key, e.Value)
			}
		}
	}
	return nil
}

func (f *Freesia) Get(e *entry.Entry) error {
	if e.EnableLocalCache() {
		data, err := f.cache.Get(e.Key)
		switch err {
		case roc.ErrMiss:
		case nil:
			b, ok := data.([]byte)
			if err = e.Decode(b); ok && err != nil {
				return errors.Wrapf(err, "decode key = %s, data = %s", e.Key, b)
			}
			return nil
		default:
			return err
		}
	}
	b, err := f.store.Get(e.Key).Bytes()
	switch err {
	case nil:
		err = e.Decode(b)
		if err != nil {
			return errors.Wrapf(err, "decode key = %s, data = %s", e.Key, b)
		}
		return nil
	default:
		return errors.Wrapf(err, "store get key = %s", e.Key)
	}
}

func (f *Freesia) batchGet(es ...*entry.Entry) ([]*entry.Entry, error) {
	pipe := f.store.Pipeline()
	found := make(map[*entry.Entry]struct{})
	ret := make(map[*redis.StringCmd]*entry.Entry)
	for _, e := range es {
		if e.EnableLocalCache() {
			b, err := f.cache.Get(e.Key)
			switch err {
			case roc.ErrMiss:
				cmd := pipe.Get(e.Key)
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
			cmd := pipe.Get(e.Key)
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
				return nil, err
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

func (f *Freesia) Del(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := f.store.Del(keys...).Result()
	if err != nil {
		return errors.Wrapf(err, "store del, keys = %+v", keys)
	}
	for _, key := range keys {
		if err := f.cache.Del(key); err != nil {
			return errors.Wrapf(err, "delete cache: key = %s", key)
		}
	}
	return nil
}

func (f *Freesia) Pub(keys ...string) error {
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
		for message := range f.pubsub.Channel() {
			job := curlew.NewJob()
			job.Arg = message
			job.Fn = func(ctx context.Context, arg interface{}) error {
				message := arg.(*redis.Message)
				var keys []string
				if err := msgpack.Unmarshal([]byte(message.Payload), &keys); err != nil {
					return err
				}
				for _, key := range keys {
					if err := f.cache.Del(key); err != nil {
						fmt.Printf("async delete cache key = %s, err = %#v", key, err)
					}
				}
				return nil
			}
			f.dispatcher.SubmitAsync(job)
		}
	}()

}
