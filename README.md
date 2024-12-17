# comby-nats-broker

Implementation of the Broker interface defined in [comby](https://github.com/gradientzero/comby) with NATS. **comby** is a powerful application framework designed with Event Sourcing and Command Query Responsibility Segregation (CQRS) principles, written in Go.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Prerequisites

- [Golang 1.22+](https://go.dev/dl/)
- [comby](https://github.com/gradientzero/comby)
- [NATS](https://nats.io/download/)

```shell
# run NATS locally for testings
docker run -d --name nats-main -p 4222:4222 -p 6222:6222 -p 8222:8222 nats
```

## Installation

*comby-nats-broker* supports the latest version of comby (v2), requires Go version 1.22+ and is based on NATS client [nats.go](https://github.com/nats-io/nats.go).

```shell
go get github.com/gradientzero/comby-nats-broker
```

## Quickstart

```go
import (
	"github.com/gradientzero/comby-nats-broker"
	"github.com/gradientzero/comby/v2"
)

// create NATS broker
natsBroker := broker.NewBroker("nats://127.0.0.1:4222")

// create Facade
fc, _ := comby.NewFacade(
  comby.FacadeWithBroker(natsBroker),
)
```

## Tests

```shell
go test -v ./...
```

## Contributing
Please follow the guidelines in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](./LICENSE.md).

## Contact
https://www.gradient0.com
