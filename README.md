# Go client for Mackerel Cache

Go client for [Mackerel Cache](https://github.com/chadjefferies/mackerelcache).

This library is in alpha state. While most basic cache features are implemented, the more advanced features such as scanning and watching are not yet supported. See [issues](https://github.com/chadjefferies/go-mackerelcache/issues) for more details.

## Quickstart

Install with:

```shell
$ go get github.com/chadjefferies/go-mackerelcache/mackerelcache
```

Then use it like:

```go
import (
    "context"
    "github.com/chadjefferies/go-mackerelcache/mackerelcache"
)

func main() {
    var ctx = context.Background()
	client, err := mackerelcache.NewClient(&mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   1 * time.Minute,
	})
	defer client.Close()

	stringCache := mackerelcache.NewStringCache(client)
	err = stringCache.Put(ctx, "myapp", "mykey", "my value")
	...

	value, err := stringCache.Get(ctx, "myapp", "mykey")
	...
}
```