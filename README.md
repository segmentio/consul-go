# consul-go

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

## Usage

```go
package main

import (
    "fmt"
    "context"

    "github.com/segmentio/consul-go"
)

func main() {
    // Creates a client, it uses the default consul transport and sends rquests
    // to the Consul agent at localhost:8500.
    client := consul.Client{}

    sid, err := client.CreateSession(context.Background(), consul.SessionConfig{
        LockDelay: "15s",
        Behavior:  consul.Release,
    })

    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println(sid)
    }
}
```
