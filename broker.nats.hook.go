package broker

import (
	"context"
	"time"

	"github.com/gradientzero/comby/v2"
	"github.com/nats-io/nats.go"
)

// fullfilling base.HookBus interface
func (b *broker) SubscribeHook(h *comby.Hook) error {
	var err error
	var sub *nats.Subscription
	if sub, err = b.natsConn.Subscribe(h.CmdUuid, func(m *nats.Msg) {
		logger.Debug(
			"SubscribeHook",
			"broker-uuid", b.BrokerUuid,
			"subject", h.CmdUuid,
			"data-length", len(m.Data),
		)

		// deserialize received reply
		hookResult := &comby.HookResult{}
		if hookResult, err = comby.Deserialize(m.Data, hookResult); err != nil {
			logger.Error("SubscribeHook", "err", err)
		}

		// call hook result callback
		if val, ok := b.hooks.Load(h.CmdUuid); ok {
			_h, ok := val.(*comby.Hook)
			if ok {
				ctx := context.Background()
				ctx, _ = context.WithTimeout(ctx, 300*time.Second)
				_h.Result(ctx, hookResult)
			}
		}
	}); err != nil {
		return err
	}

	// add nats subscription to map to unsubscribe later
	b.subs.Store(h.CmdUuid, sub)

	// add hook to map to be able to retrieve it later
	b.hooks.Store(h.CmdUuid, h)
	return nil
}

func (b *broker) UnsubscribeHook(cmdUuid string) error {
	// unsubscribe from nats subscription
	if subVal, ok := b.subs.Load(cmdUuid); ok {
		sub, ok := subVal.(*nats.Subscription)
		if ok {
			if err := sub.Unsubscribe(); err != nil {
				return err
			}
			b.subs.Delete(cmdUuid)
		}
	}

	// remove hook from hooks map and keep hook untouched
	b.hooks.Delete(cmdUuid)
	return nil
}

func (b *broker) PublishHook(cmdUuid string, dataBytes []byte) error {
	// messages are always sent to a specific subject (here cmdUuid)
	// The instance identification (=BrokerUuid) is additionally packed in
	// the header in order to avoid double processing of this message.
	msg := nats.NewMsg(cmdUuid)
	msg.Data = dataBytes
	msg.Header.Add(KEY_BROKER_UUID, b.BrokerUuid)
	logger.Debug(
		"PublishHook",
		"broker-uuid", b.BrokerUuid,
		"subject", cmdUuid,
		"data-length", len(dataBytes),
	)
	if err := b.natsConn.PublishMsg(msg); err != nil {
		return err
	}
	return nil
}
