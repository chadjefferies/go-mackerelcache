package encoding

type Codec[T any] interface {
	Encode(v T) ([]byte, error)
	Decode(data []byte) (T, error)
}

type StringCodec[T string] struct {
}

func (c *StringCodec[string]) Encode(v string) ([]byte, error) {
	return []byte(v), nil
}

func (c *StringCodec[string]) Decode(data []byte) (string, error) {
	return string(data), nil
}
