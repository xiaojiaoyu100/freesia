package entry

import (
	"errors"
	"time"

	"github.com/xiaojiaoyu100/freesia/codec"
)

type Entry struct {
	Key              string
	Value            interface{}
	Expiration       time.Duration
	data             []byte
	codec            codec.Codec
	enableLocalCache bool
}

func New(key string, value interface{}, expiration time.Duration) (*Entry, error) {
	if key == "" {
		return nil, errors.New("key length must greater than zero")
	}

	if expiration.String() == "0s" {
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

func KS(es ...*Entry) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, e := range es {
		ret[e.Key] = e.Value
	}
	return ret
}
