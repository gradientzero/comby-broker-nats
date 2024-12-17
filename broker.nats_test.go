package broker_test

import (
	"context"
	"sync"
	"testing"

	broker "github.com/gradientzero/comby-nats-broker"
	"github.com/gradientzero/comby/v2"
)

func TestBrokerNats1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init broker
	commandBus := comby.NewCommandBusMemory()
	eventBus := comby.NewEventBusMemory()
	natsBroker := broker.NewBroker("nats://127.0.0.1:4222")
	if err = natsBroker.Init(ctx,
		comby.BrokerOptionWithAppName("my-app"),
		comby.BrokerOptionWithCommandBus(commandBus),
		comby.BrokerOptionWithEventBus(eventBus),
	); err != nil {
		t.Fatal(err)
	}

	if err := natsBroker.PublishToCommandBus(ctx, []byte("my-command")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _cmd := range natsBroker.SubscribeToCommandBus() {
			if string(_cmd) == "my-command" {
				t.Log("received command")
				wg.Done()
			}
		}
	}()
	wg.Wait()
}
