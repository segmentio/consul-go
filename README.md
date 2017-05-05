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
