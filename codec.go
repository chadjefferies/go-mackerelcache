package mackerelcache

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

type BinaryCodec[T []byte] struct {
}

func (c *BinaryCodec[T]) Encode(v []byte) ([]byte, error) {
	return v, nil
}

func (c *BinaryCodec[T]) Decode(data []byte) ([]byte, error) {
	return data, nil
}
