package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// fullfilling base.EventBus interface
func (b *broker) PublishToEventBus(ctx context.Context, dataBytes []byte) error {
	// Events (as one bulk) are always sent to a specific subject (=PublishEventBusId)
	// The instance identification (=BrokerUuid) is additionally packed in
	// the header in order to avoid double processing of this event bulk.
	msg := nats.NewMsg(b.PublishEventBusId)
	msg.Data = dataBytes
	msg.Header.Add(KEY_BROKER_UUID, b.BrokerUuid)
	logger.Debug(
		"PublishToEventBus",
		"broker-uuid", b.BrokerUuid,
		"subject", b.PublishEventBusId,
		"data-length", len(dataBytes),
	)
	if err := b.natsConn.PublishMsg(msg); err != nil {
		return err
	}
	return nil
}

func (b *broker) SubscribeToEventBus() <-chan []byte {
	// To prevent out-of-scope events, all events are sent in bulk.
	// This means that a command can generate multiple events so that
	// they are processed as one unit.

	b.natsConn.Subscribe(b.SubscribeEventBusId, func(m *nats.Msg) {
		//
		// In the pub sub system, different sub-applications can listen to the
		// same subjects. For example, an app can send information to the
		// subject "sub.1", but all applications listening to the
		// subject "sub.*" receive it. That's why we should differentiate
		// again internally whether certain messages are really addressed to us.
		//
		msgFromThisApplication := m.Subject == b.SubscribeEventBusId

		// avoid double handling - broker uuid unique for each instance
		msgFromThisApplicationInstance := m.Header.Get(KEY_BROKER_UUID) == b.BrokerUuid

		// The event is valid if it comes from the same applications but not from this instance
		msgValid := !msgFromThisApplicationInstance

		if msgValid {
			logger.Debug(
				"SubscribeToEventBus",
				"broker-uuid", b.BrokerUuid,
				"subject", b.PublishEventBusId,
				"data-length", len(m.Data),
			)
			if msgFromThisApplication {
				if b.EventBus != nil {
					ctx := context.Background()
					ctx, _ = context.WithTimeout(ctx, 300*time.Second)
					if err := b.EventBus.PublishToEventBus(ctx, m.Data); err != nil {
						logger.Error("SubscribeToEventBus", "err", err)
					}
				}
			}
		}
	})
	return nil
}
