package freesia

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/xiaojiaoyu100/curlew"
	"github.com/xiaojiaoyu100/freesia/singleflight"
	"github.com/xiaojiaoyu100/roc"
)

type Freesia struct {
	Store      Store
	codec      Codec
	keyColl    map[string]*KeyMeta
	cache      *roc.Cache
	group      *singleflight.Group
	dispatcher *curlew.Dispatcher
	monitor    Monitor
}

func New(store Store, setters ...Setter) (*Freesia, error) {
	cache, err := roc.New()
	if err != nil {
		return nil, err
	}
	f := new(Freesia)
	f.Store = store
	f.codec = MessagePackCodec{}
	f.keyColl = make(map[string]*KeyMeta)
	for _, setter := range setters {
		if err := setter(f); err != nil {
			return nil, err
		}
	}
	if f.codec == nil {
		return nil, errors.New("no codec provided")
	}

	if f.monitor == nil {
		return nil, errors.New("no monitor provided")
	}

	f.cache = cache
	f.group = &singleflight.Group{}
	f.dispatcher, err = curlew.New()
	if err != nil {
		return nil, err
	}
	f.sub()
	return f, nil
}

func (f *Freesia) Config(key string, s *KeyMeta) error {
	_, ok := f.keyColl[key]
	if ok {
		return errDupKey
	}
	if s.Expiration.Seconds() < 2 {
		return errKeyExpirationTooShort
	}
	f.keyColl[key] = s
	return nil
}

func (f *Freesia) Set(key string, value interface{}) error {
	keyMeta, err := f.keyMeta(key)
	if err != nil {
		return err
	}
	b, err := f.codec.Encode(value)
	if err != nil {
		return err
	}
	if err := f.Store.Set(key, b, keyMeta.Expiration).Err(); err != nil {
		return err
	}
	if keyMeta.EnableLocalCache {
		if err := f.cache.Set(key, b, keyMeta.Expiration); err != nil {
			return err
		}
	}
	return nil
}

func (f *Freesia) Get(key string, value interface{}) error {
	keyMeta, err := f.keyMeta(key)
	if err != nil {
		return err
	}
	if keyMeta.EnableLocalCache {
		data, err := f.cache.Get(key)
		if err == nil {
			b := data.([]byte)
			if err := f.codec.Decode(b, value); err != nil {
				return err
			}
			return nil
		}
	}
	if keyMeta.SingleFlight {
		i, err := f.group.Do(key, func() (i interface{}, e error) {
			b, err := f.Store.Get(key).Bytes()
			if err != nil {
				return nil, err
			}
			return b, nil
		})
		if err != nil {
			return err
		}
		b := i.([]byte)
		if err := f.codec.Decode(b, value); err != nil {
			return err
		}
		if keyMeta.EnableLocalCache {
			if err := f.cache.Set(key, b, keyMeta.Expiration); err != nil {
				return err
			}
		}
	} else {
		b, err := f.Store.Get(key).Bytes()
		if err != nil {
			return err
		}
		if err := f.codec.Decode(b, value); err != nil {
			return err
		}
		if keyMeta.EnableLocalCache {
			if err := f.cache.Set(key, b, keyMeta.Expiration); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Freesia) Delete(key string) error {
	_, err := f.keyMeta(key)
	if err != nil {
		return err
	}

	ret, err := f.Store.Del(key).Result()
	if err != nil {
		return err
	}

	if err := f.cache.Del(key); err != nil {
		return err
	}

	if ret == 0 {
		return errKeyMiss
	}

	return nil
}

func (f *Freesia) keyMeta(key string) (*KeyMeta, error) {
	keyMeta, ok := f.keyColl[key]
	if !ok {
		return nil, errMissingKeyMeta
	}
	return keyMeta, nil
}

func (f *Freesia) sub() {
	go func() {
		pubSub := f.Store.Subscribe(channel)
		defer func() {
			if err := pubSub.Close(); err != nil {
				f.monitor(fmt.Errorf("close pubsub err = %v", err))
			}
		}()
		for {
			select {
			case message := <-pubSub.Channel():
				{
					job := curlew.NewJob()
					job.Arg = message
					job.Fn = func(ctx context.Context, arg interface{}) error {
						message := arg.(*redis.Message)
						if message.Channel == channel {
							if _, err := f.keyMeta(message.Payload); err == nil {
								if err := f.Delete(message.Payload); err != nil {
									f.monitor(fmt.Errorf("delete cache key = %s, err = %v", message.Payload, err))
								}
							}
						}
						return nil
					}
					f.dispatcher.SubmitAsync(job)
				}
			}
		}
	}()

}
