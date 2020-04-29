package entry

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/xiaojiaoyu100/freesia/codec"
)

// Entry a cache entry.
type Entry struct {
	key              string
	value            interface{}
	exp              time.Duration
	localExp         time.Duration
	data             []byte
	ttl              float64
	loadFrom         int
	codec            codec.Codec
	enableLocalCache bool
}

// New generates an entry.
func New(setting ...Setting) (*Entry, error) {
	e := &Entry{
		codec: codec.MessagePackCodec{},
	}
	for _, o := range setting {
		err := o(e)
		if err != nil {
			return nil, err
		}
	}
	if e.key == "" {
		return nil, errors.New("key length must greater than zero")
	}

	return e, nil
}

// Key returns the key.
func (e *Entry) Key() string {
	return e.key
}

// Encode encodes an entry.
func (e *Entry) Encode() error {
	if err := checkSet(e); err != nil {
		return err
	}
	b, ok := e.value.([]byte)
	if ok {
		e.data = b
		return nil
	}
	b, err := e.codec.Encode(e.value)
	if err != nil {
		return fmt.Errorf("key = %s, encode err: %w", e.key, err)
	}
	e.data = b
	return nil
}

// Decode decodes an entry.
func (e *Entry) Decode(data []byte) error {
	if err := checkGet(e); err != nil {
		return err
	}
	if err := e.codec.Decode(data, &e.value); err != nil {
		return err
	}
	e.data = data
	return nil
}

// Data entry data.
func (e *Entry) Data() []byte {
	return e.data
}

// Value entry value.
func (e *Entry) Value() interface{} {
	return e.value
}

// EnableLocalCache enables the local expiration.
func (e *Entry) EnableLocalCache() bool {
	return e.enableLocalCache
}

// LocalExp local expiration.
func (e *Entry) LocalExp() time.Duration {
	return e.localExp
}

// Exp expiration.
func (e *Entry) Exp() time.Duration {
	return e.exp
}

// TTL 只会返回redis的存活时间，与SourceCenter一起使用。
func (e *Entry) TTL() float64 {
	return e.ttl
}

// SetTTL sets ttl for an entry.
func (e *Entry) SetTTL(ttl float64) {
	e.ttl = ttl
}

// SourceLocal reports cache from local.
func (e *Entry) SourceLocal() bool {
	return e.loadFrom == SourceLocal
}

// SetSource sets source.
func (e *Entry) SetSource(s int) {
	e.loadFrom = s
}

// SourceCenter reports cache from center.
func (e *Entry) SourceCenter() bool {
	return e.loadFrom == SourceCenter
}

// Reset resets a entry.
func (e *Entry) Reset() {
	e.value = nil
	e.data = e.data[:0]
	e.ttl = 0
	e.loadFrom = sourceUnknown
}

// CheckSet checks set conditions.
func checkSet(e *Entry) error {
	if int64(e.exp) == 0 {
		return errors.New("exp must be greater than zero")
	}
	if e.enableLocalCache && int64(e.localExp) == 0 {
		return errors.New("local exp must be greater than zero when enabling local cache")
	}
	if e.value == nil {
		return errors.New("value must be non nil")
	}
	return nil
}

func checkGet(e *Entry) error {
	if reflect.TypeOf(e.value).Kind() != reflect.Ptr {
		return fmt.Errorf("%s: need pointer", e.key)
	}
	if reflect.ValueOf(e.value).IsNil() {
		return fmt.Errorf("%s: need pointer to a actual struct", e.key)
	}
	return nil
}

// KS key value map.
func KS(es ...*Entry) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, e := range es {
		ret[e.key] = e.value
	}
	return ret
}
