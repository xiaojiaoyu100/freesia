package freesia

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"
	"github.com/xiaojiaoyu100/curlew"
	"github.com/xiaojiaoyu100/freesia/entry"
	"github.com/xiaojiaoyu100/freesia/singleflight"
	"github.com/xiaojiaoyu100/roc"
)

type Freesia struct {
	store      Store
	cache      *roc.Cache
	group      *singleflight.Group
	dispatcher *curlew.Dispatcher
	monitor    Monitor
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

	f.group = &singleflight.Group{}

	monitor := func(err error) {}
	f.dispatcher, err = curlew.New(curlew.WithMonitor(monitor))
	if err != nil {
		return nil, err
	}

	f.sub()

	if f.monitor == nil {
		return nil, errors.New("no monitor provided")
	}

	return f, nil
}

func (f *Freesia) Set(e *entry.Entry) error {
	if err := e.Encode(); err != nil {
		f.monitor(fmt.Errorf("encode err: %#v, key = %s, value = %#v", err, e.Key, e.Value))
		return err
	}
	if err := f.store.Set(e.Key, e.Data(), e.Expiration).Err(); err != nil {
		return err
	}
	if e.EnableLocalCache() {
		if err := f.cache.Set(e.Key, e.Data(), e.Expiration/2); err != nil {
			f.monitor(fmt.Errorf("cache set err: %#v, key = %s, value = %#v", err, e.Key, e.Value))
			return err
		}
	}
	return nil
}

func (f *Freesia) MSet(es ...*entry.Entry) error {
	pipe := f.store.Pipeline()
	for _, e := range es {
		if err := e.Encode(); err != nil {
			f.monitor(fmt.Errorf("encode err: %#v, key = %s, value = %#v", err, e.Key, e.Value))
			return err
		}
		pipe.Set(e.Key, e.Data(), e.Expiration)
	}
	_, err := pipe.Exec()
	if err != nil {
		return err
	}
	for _, e := range es {
		if e.EnableLocalCache() {
			err := f.cache.Set(e.Key, e.Data(), e.Expiration)
			if err != nil {
				f.monitor(fmt.Errorf("cache set err: %#v, key = %s, value = %#v", err, e.Key, e.Value))
				return err
			}
		}
	}
	return nil
}

func (f *Freesia) Get(e *entry.Entry) error {
	if e.EnableLocalCache() {
		data, err := f.cache.Get(e.Key)
		if err == nil {
			b, ok := data.([]byte)
			if err := e.Decode(b); ok && err != nil {
				f.monitor(fmt.Errorf("decode err: %#v, key = %s, value = %#v", err, e.Key, e.Value))
				return err
			}
			return nil
		}
	}
	fn := func() (interface{}, error) {
		b, err := f.store.Get(e.Key).Bytes()
		if err != nil {
			return nil, err
		}
		if err := e.Decode(b); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if e.SingleFlight() {
		_, err := f.group.Do(e.Key, fn)
		if err != nil {
			return err
		}
	} else {
		_, err := fn()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Freesia) Delete(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := f.store.Del(keys...).Result()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := f.cache.Del(key); err != nil {
			f.monitor(fmt.Errorf("local cache: key = %s, err = %#v", key, err))
		}
	}
	return nil
}

func (f *Freesia) sub() {
	go func() {
		pubSub := f.store.Subscribe(channel)
		defer func() {
			if err := pubSub.Close(); err != nil {
				f.monitor(fmt.Errorf("close pubsub err = %v", err))
			}
		}()
		for message := range pubSub.Channel() {
			job := curlew.NewJob()
			job.Arg = message
			job.Fn = func(ctx context.Context, arg interface{}) error {
				message := arg.(*redis.Message)
				var keys []string
				if err := msgpack.Unmarshal([]byte(message.Payload), keys); err != nil {
					return err
				}
				if err := f.Delete(keys...); err != nil {
					f.monitor(fmt.Errorf("delete cache key = %s, err = %v", message.Payload, err))
				}
				return nil
			}
			f.dispatcher.SubmitAsync(job)
		}
	}()

}
