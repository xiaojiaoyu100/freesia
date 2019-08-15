package freesia

import "github.com/vmihailenco/msgpack"

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type MessagePackCodec struct {
}

func (codec MessagePackCodec) Encode(value interface{}) ([]byte, error) {
	return msgpack.Marshal(value)
}

func (codec MessagePackCodec) Decode(data []byte, value interface{}) error {
	return msgpack.Unmarshal(data, value)
}
