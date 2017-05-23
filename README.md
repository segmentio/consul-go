# consul-go [![CircleCI](https://circleci.com/gh/segmentio/consul-go.svg?style=shield)](https://circleci.com/gh/segmentio/consul-go) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/consul-go)](https://goreportcard.com/report/github.com/segmentio/consul-go) [![GoDoc](https://godoc.org/github.com/segmentio/consul-go?status.svg)](https://godoc.org/github.com/segmentio/consul-go)

## Motivations

Consul being built in Go it already has pretty good client library support,
however the package was written a while ago and still lack modern Go features.  
This package exposes an API for interacting with a consul agent. It differs from
the standard Go package in a couple of ways:

- The abstractions are not one-to-one translations of the Consul API, instead
the package offers building blocks that can be used to interract with Consul.

- Arguments are passed by value which makes the code easier to manipulate,
safer (no risk of dereferencing nil pointers), and greatly reduces the number
of dynamic memory allocations.

- The `Client` type borrows its design from the `net/http` package and makes the
its use more idiomatic to Go developers.

- The client methods all support passing a `context.Context`, allowing finer
grain control over requests timeout and cancellations.

## Resolver

One of the main features of Consul is service discovery, which means translating
a logical service name into a set of network addresses at which clients can
access it.  
The package offers a high-level abstraction to address this specific use-case
with the `Resolver` type.

```go
package main

import (
    "context"
    "fmt"

    "github.com/segmentio/consul-go"
)

func main() {
    // Queries Consul for a list of addresses where "my-service" is available,
    // the result will be sorted to get the addresses closest to the agent first.
    rslv := &Resolver{
        Near: "_agent",
    }

    addrs, err := rslv.LookupService(context.Background(), "my-service")
    if err != nil {
        fmt.Println(err)
        return
    }

    for _, addr := range addrs {
        fmt.Println(addr)
    }
}
```

## Dialer

Resolving service names to addresses is often times done because the program
intends to connect to those services.  
The package provides an abstractions of this mechanism with the `Dialer` type,
which mirror the standard `net.Dialer` to make it an easy drop-in replacement
and bring service discovery to existing software.

Here's an example of how the `Dialer` type can be paired with the standard HTTP
client:
```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "os"

    "github.com/segmentio/consul-go"
)

func main() {
    // Replace the DialContext method on the default transport to use consul for
    // all host name resolutions.
    http.DefaultTransport.DialContext = (&consul.Dialer{
        Timeout:   30 * time.Second,
        KeepAlive: 30 * time.Second,
        DualStack: true,
    }).DialContext

    res, err := http.Get("http://my-service/")
    if err != nil {
        fmt.Println(err)
        return
    }

    io.Copy(os.Stdout, res.Body)
    res.Body.Close()
}
```

## Listener

On the other side, services also need to register to consul. While there are
ways to automate this using tools like [registrator](http://gliderlabs.github.io/registrator/latest/)
some systems may need to have finer grain control over the metadata attached to
the service registration. 
This is where the `Listener` type comes into play. It allows the creation of
`net.Listener` values that are automatically registered to Consul.

Here's an example of how the `Listener` type can be paired with the standard
HTTP server:
```go
package main

import (
    "context"
    "fmt"
    "net/http"

    "github.com/segmentio/consul-go"
)

func main() {
    // This listener automatically registers the IP and port that it accepts
    // connections on.
    httpLstn, err := consul.Listen("tcp", ":0")
    if err != nil {
        fmt.Println(err)
        return
    }

    (&http.Server{
        Handler: http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
            // ...
        }),
    }).Serve(httpLstn)
}
```

## Transport (HTTP)

The approach of overwritting the dialer in the HTTP transport may not always be
ideal because of connection pooling, there will be a *sticky* effect where all
requests going out for a single hostname would hit the same services. This is
due to the fact that once the connection is established no more consul lookups
are made to resolve service names.  
An alternative option is to make a service resolution call for every request,
which may resolve to different network addresses and better distribute the load
among the pool of available services. The `httpconsul` package has a decorator
that is intended to transparently provide this feature on `http.RoundTripper`
instances.

```go
package main

import (
    "net/http"

    "github.com/segmentio/consul-go/httpconsul"
)

func main() {
    // Wraps the default transport so all service names are looked up in consul.
    // The consul client uses its own transport so there's no risk of recursive
    // loop here.
    http.DefaultTransport = httpconsul.NewTransport(http.DefaultTransport)

    // ...
}
```

## Sessions and Locks

Sessions and Locks have lifetimes, which translates nicely into the Go Context
concept. The APIs abstract sessions and locks as contexts, which makes it possible
to inject dependencies on Consul Sessions and Locks into any context-aware code.

The synchronization mechanisms come in various locking algorithms (see `Lock`,
and other similar functions).
Lock takes a list of keys and blocks until it was able to acquire all of them,
the algorithm is designed to prevent deadlocks (by sorting the list of keys and
acquiring the locks sequentially).

Here are a couple of examples:

### Creating Sessions
```go
// Creates a session in Consul and returns a context associated to it.
// The session is automatically renewed, and destroyed when cancel is called.
//
// If the session gets expired or removed for some reason the context is
// asynchronously canceled.
ctx, cancel := consul.WithSession(context.Background(), consul.Session{
  Name: "my session",
})
```

### Acquiring Locks
```go
// A session is automatically created and attached to the keys, if the session
// expires it also releases the locks which means the returned context would
// get asynchronously canceled. This is great to build algorithms that depend
// on the lock being held and need to abort their execution if they detect that
// they lost ownership of the keys.
ctx, cancel := consul.Lock(context.Background(), "key-1", "key-A")
```

### Chaining dependencies
```go
// This context is canceled after 10 seconds, it's the parent context of the
// session which means it expires the session after 10 seconds.
deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// ...
session, destroy := consul.WithSession(deadline, consul.Session{
  Name: "my session",
})
// By passing the session as parent context we can attach the session to
// multiple locks, if it expires, all those locks are released and their
// contexts are canceled.
lock1, release1 := consul.Lock(session, "key-1")
lock2, release2 := consul.Lock(session, "key-2")
```
