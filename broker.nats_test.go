package broker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	broker "github.com/gradientzero/comby-broker-nats"
	"github.com/gradientzero/comby/v2"
)

func TestBrokerNats1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init broker
	commandBus := comby.NewCommandBusMemory()
	eventBus := comby.NewEventBusMemory()
	natsBroker1 := broker.NewBrokerNats("nats://127.0.0.1:4222")
	if err = natsBroker1.Init(ctx,
		comby.BrokerOptionWithAppName("my-app"),
		comby.BrokerOptionWithCommandBus(commandBus),
		comby.BrokerOptionWithEventBus(eventBus),
	); err != nil {
		t.Fatal(err)
	}

	// Pretend I'm another instance
	natsBroker2 := broker.NewBrokerNats("nats://127.0.0.1:4222")
	if err = natsBroker2.Init(ctx,
		comby.BrokerOptionWithAppName("my-app"),
		comby.BrokerOptionWithCommandBus(commandBus),
		comby.BrokerOptionWithEventBus(eventBus),
	); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _cmd := range commandBus.SubscribeToCommandBus() {
			if string(_cmd) == "my-command" {
				t.Log("received command")
				wg.Done()
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if err := natsBroker1.PublishToCommandBus(ctx, []byte("my-command")); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}
