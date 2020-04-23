package codec

import "github.com/vmihailenco/msgpack"

// Codec standards for an codec.
type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

// MessagePackCodec represents a msg pack codec.
type MessagePackCodec struct {
}

// Encode encodes a value.
func (codec MessagePackCodec) Encode(value interface{}) ([]byte, error) {
	return msgpack.Marshal(value)
}

// Decode decodes a value.
func (codec MessagePackCodec) Decode(data []byte, value interface{}) error {
	return msgpack.Unmarshal(data, value)
}
