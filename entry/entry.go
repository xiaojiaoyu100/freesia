package entry

import (
	"errors"
	"fmt"
	"github.com/xiaojiaoyu100/freesia/codec"
	"reflect"
	"time"
)

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

func (e *Entry) Data() []byte {
	return e.data
}

func (e *Entry) Value() interface{} {
	return e.value
}

// EnableLocalExp enables the local expiration.
func (e *Entry) EnableLocalExp() bool {
	return e.enableLocalCache
}

func (e *Entry) LocalExp() time.Duration {
	return e.localExp
}

func (e *Entry) Exp() time.Duration {
	return e.exp
}

func (e *Entry) TTL() float64 {
	return e.ttl
}

func (e *Entry) SetTTL(ttl float64) {
	e.ttl = ttl
}

func (e *Entry) SourceLocal() bool {
	return e.loadFrom == SourceLocal
}

func (e *Entry) SetSource(s int) {
	e.loadFrom = s
}

func (e *Entry) SourceCenter() bool {
	return e.loadFrom == SourceCenter
}

// CheckSet checks set conditions.
func checkSet(e *Entry) error {
	if int64(e.exp) == 0 {
		return errors.New("exp must be greater than zero")
	}
	if e.enableLocalCache && int64(e.localExp) == 0 {
		return errors.New("local exp must be greater than zero when enabling local cache")
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

func KS(es ...*Entry) map[string]interface{} {
	ret := make(map[string]interface{})
	for _, e := range es {
		ret[e.key] = e.value
	}
	return ret
}
