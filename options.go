package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/duolacloud/broker-core"
	"github.com/huandu/go-clone"
)

var (
	DefaultBrokerConfig  = sarama.NewConfig()
	DefaultClusterConfig = sarama.NewConfig()
)

type brokerConfigKey struct{}
type clusterConfigKey struct{}

func BrokerConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(brokerConfigKey{}, c)
}

func ClusterConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(clusterConfigKey{}, c)
}

type asyncProduceErrorKey struct{}
type asyncProduceSuccessKey struct{}

func AsyncProducer(errors chan<- *sarama.ProducerError, successes chan<- *sarama.ProducerMessage) broker.Option {
	// set default opt
	var opt = func(options *broker.Options) {}
	if successes != nil {
		opt = setBrokerOption(asyncProduceSuccessKey{}, successes)
	}
	if errors != nil {
		opt = setBrokerOption(asyncProduceErrorKey{}, errors)
	}
	return opt
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption.
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type subscribeConfigKey struct{}

func SubscribeConfig(c *sarama.Config) broker.SubscribeOption {
	return setSubscribeOption(subscribeConfigKey{}, c)
}

// consumerGroupHandler is the implementation of sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	handler broker.Handler
	subopts broker.SubscribeOptions
	kopts   broker.Options
	cg      sarama.ConsumerGroup
	// sess    sarama.ConsumerGroupSession
}

func (*consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		p := &publication{t: msg.Topic, km: msg, cg: h.cg, sess: sess}
		eh := h.kopts.ErrorHandler

		if h.subopts.ResultType != nil {
			p.m = clone.Clone(h.subopts.ResultType)
			if err := h.kopts.Codec.Unmarshal(msg.Value, p.m); err != nil {
				p.err = err
				p.m = msg.Value
				if eh != nil {
					eh(p)
				} else {
					fmt.Printf("[kafka]: failed to unmarshal: %v", err)
				}
				continue
			}
		}

		if p.m == nil {
			p.m = msg.Value
		}

		err := h.handler(p)
		if err == nil && h.subopts.AutoAck {
			sess.MarkMessage(msg, "")
		} else if err != nil {
			p.err = err
			if eh != nil {
				eh(p)
			} else {
				fmt.Printf("[kafka]: subscriber error: %v", err)
			}
		}
	}
	return nil
}
