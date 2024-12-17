package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// fullfilling base.CommandBus interface
func (b *broker) PublishToCommandBus(ctx context.Context, dataBytes []byte) error {
	// Commands are always sent to a specific subject (=PublishCommandBusId)
	// The instance identification (=BrokerUuid) is additionally packed in
	// the header in order to avoid double processing of this commands.
	msg := nats.NewMsg(b.PublishCommandBusId)
	msg.Data = dataBytes
	msg.Header.Add(KEY_BROKER_UUID, b.BrokerUuid)
	logger.Debug(
		"PublishToCommandBus",
		"broker-uuid", b.BrokerUuid,
		"subject", b.PublishCommandBusId,
		"data-length", len(dataBytes),
		"data", string(dataBytes),
	)
	if err := b.natsConn.PublishMsg(msg); err != nil {
		return err
	}
	return nil
}

func (b *broker) SubscribeToCommandBus() <-chan []byte {
	b.natsConn.Subscribe(b.SubscribeCommandBusId, func(m *nats.Msg) {
		//
		// In the pub sub system, different sub-applications can listen to the
		// same subjects. For example, an app can send information to the
		// subject "sub.1", but all applications listening to the
		// subject "sub.*" receive it. That's why we should differentiate
		// again internally whether certain messages are really addressed to us.
		//
		// This is especially true for commands. We don't want to handle foreign commands.
		// By foreign we mean sub-applications (id, manage and main are three sub-applications)
		//
		msgFromThisApplication := m.Subject == b.PublishCommandBusId

		// avoid double handling - broker uuid unique for each instance
		msgFromThisApplicationInstance := m.Header.Get(KEY_BROKER_UUID) == b.BrokerUuid

		// The command is valid if it comes from the same application (regardless
		// of which instance) but not from this instance.
		msgValid := msgFromThisApplication && !msgFromThisApplicationInstance
		if msgValid {
			logger.Debug(
				"SubscribeToCommandBus",
				"broker-uuid", b.BrokerUuid,
				"subject", b.SubscribeCommandBusId,
				"data-length", len(m.Data),
				"data", string(m.Data),
			)
			// publish received command to internal command bus
			if b.CommandBus != nil {
				ctx := context.Background()
				ctx, _ = context.WithTimeout(ctx, 300*time.Second)
				if err := b.CommandBus.PublishToCommandBus(ctx, m.Data); err != nil {
					logger.Error("SubscribeToCommandBus", "err", err)
				}
			}
		}
	})
	return nil
}
