package broker

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/gradientzero/comby/v2"
	"github.com/nats-io/nats.go"
)

var logger = comby.Logger.With("pkg", "comby/broker", "broker", "nats")

const KEY_BROKER_UUID string = "broker-uuid"

type broker struct {
	options               comby.BrokerOptions
	Uri                   string
	PublishEventBusId     string
	PublishCommandBusId   string
	SubscribeEventBusId   string
	SubscribeCommandBusId string

	// internals (avoid double handling of commands/events)
	BrokerUuid         string
	NextSequenceNumber int64

	// NATS - message broker
	natsConn *nats.Conn
	subs     sync.Map
	hooks    sync.Map

	// connection to internal command/event bus
	CommandBus comby.CommandBus
	EventBus   comby.EventBus
}

// Make sure it implements interface
var _ comby.Broker = (*broker)(nil)

func NewBrokerNats(Uri string) comby.Broker {
	b := &broker{
		options:    comby.BrokerOptions{},
		Uri:        Uri,
		BrokerUuid: comby.NewUuid(),
		subs:       sync.Map{},
		hooks:      sync.Map{},
	}
	return b
}

func (b *broker) Init(ctx context.Context, opts ...comby.BrokerOption) error {
	for _, opt := range opts {
		if _, err := opt(&b.options); err != nil {
			return err
		}
	}
	b.PublishEventBusId = fmt.Sprintf("%s.%s", b.options.AppName, "eventbus")
	b.PublishCommandBusId = fmt.Sprintf("%s.%s", b.options.AppName, "commandbus")
	b.SubscribeEventBusId = fmt.Sprintf("%s.%s", b.options.AppName, "eventbus")
	b.SubscribeCommandBusId = fmt.Sprintf("%s.%s", b.options.AppName, "commandbus")
	/*if b.options.ReadOnly {
		b.PublishEventBusId = ""
		b.SubscribeCommandBusId = ""
	}*/
	b.CommandBus = b.options.CommandBus
	b.EventBus = b.options.EventBus

	nc, err := nats.Connect(b.Uri)
	if err != nil {
		return err
	} else {
		b.natsConn = nc
	}
	logger.Debug(
		"Connected to broker",
		"uri", obfuscateNATSUri(b.Uri),
		"read-only", b.options.ReadOnly,
		"subscribe-eventBus-id", b.SubscribeEventBusId,
		"subscribe-commandBus-id", b.SubscribeCommandBusId,
		"publish-eventBus-id", b.PublishEventBusId,
		"publish-commandBus-id", b.PublishCommandBusId,
	)

	if !b.options.ReadOnly {
		go b.SubscribeToCommandBus()
	}
	go b.SubscribeToEventBus()

	return nil
}

func (b *broker) Options() comby.BrokerOptions {
	return b.options
}

func (b *broker) String() string {
	return fmt.Sprintf("%s", obfuscateNATSUri(b.Uri))
}

func obfuscateNATSUri(uri string) string {
	// nats://userinfo@es-nats.gradient0.com
	u, _ := url.Parse(uri)
	if u != nil {
		userInfo := u.User.String()
		if len(userInfo) > 0 {
			return strings.ReplaceAll(uri, userInfo, "***")
		}
	}
	return uri
}
