package entry

import (
	"errors"
	"time"

	"gonum.org/v1/gonum/stat/distuv"

	"github.com/xiaojiaoyu100/freesia/codec"
)

const (
	zeroExpiration = "0s"
)

type Entry struct {
	Key              string
	Value            interface{}
	Expiration       time.Duration
	exp              time.Duration
	data             []byte
	codec            codec.Codec
	enableLocalCache bool
}

func New(key string, value interface{}, expiration time.Duration) (*Entry, error) {
	if key == "" {
		return nil, errors.New("key length must greater than zero")
	}

	if expiration.String() == zeroExpiration {
		return nil, errors.New("expiration must be greater than 0")
	}

	return &Entry{
		Key:        key,
		Value:      value,
		Expiration: expiration,
		codec:      codec.MessagePackCodec{},
	}, nil
}

func (e *Entry) EnableLocalCache() bool {
	return e.enableLocalCache
}

func (e *Entry) SetEnableLocalCache(b bool) {
	e.enableLocalCache = b
}

func (e *Entry) Encode() error {
	if e.data != nil {
		return nil
	}
	b, err := e.codec.Encode(e.Value)
	if err != nil {
		return err
	}
	e.data = b
	return nil
}

func (e *Entry) Decode(data []byte) error {
	if err := e.codec.Decode(data, e.Value); err != nil {
		return err
	}
	e.data = data
	return nil
}

func (e *Entry) Data() []byte {
	return e.data
}

func (e *Entry) Exp() time.Duration {
	e.lazyExp()
	return e.exp
}

func (e *Entry) LocalExp() time.Duration {
	e.lazyExp()
	return e.exp / 2
}

func (e *Entry) lazyExp() {
	if e.exp.String() == zeroExpiration {
		e.exp = time.Duration(float64(e.Expiration) * distuv.Uniform{Min: 0.8, Max: 1.2}.Rand())
	}
}

func KS(es ...*Entry) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, e := range es {
		ret[e.Key] = e.Value
	}
	return ret
}
