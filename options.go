package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/duolacloud/broker-core"
)

var (
	DefaultBrokerConfig  = sarama.NewConfig()
	DefaultClusterConfig = sarama.NewConfig()
)

type brokerConfigKey struct{}
type clusterConfigKey struct{}
type shardingKey struct{}

func BrokerConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(brokerConfigKey{}, c)
}

func ClusterConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(clusterConfigKey{}, c)
}

type shardingKeyConfigKey struct{}

func WithShardingKey(shardingKey string) broker.PublishOption {
	return setPublishOption(shardingKeyConfigKey{}, shardingKey)
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

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type subscribeConfigKey struct{}

func SubscribeConfig(c *sarama.Config) broker.SubscribeOption {
	return setSubscribeOption(subscribeConfigKey{}, c)
}

// consumerGroupHandler is the implementation of sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	ctx     context.Context
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
		var m broker.Message
		p := &publication{m: &m, t: msg.Topic, km: msg, cg: h.cg, sess: sess}
		eh := h.kopts.ErrorHandler

		if h.kopts.Codec != nil {
			if err := h.kopts.Codec.Unmarshal(msg.Value, &m); err != nil {
				p.err = err
				p.m.Body = msg.Value
				if eh != nil {
					eh(h.ctx, p)
				} else {
					// log.Errorf("[kafka]: failed to unmarshal: %v", err)
					fmt.Printf("[kafka] failed to unmarshal: %v\n", err)
				}
				continue
			}
		}

		if p.m.Body == nil {
			p.m.Body = msg.Value
		}
		// if we don't have headers, create empty map
		if m.Header == nil {
			m.Header = make(map[string]string)
		}
		for _, header := range msg.Headers {
			m.Header[string(header.Key)] = string(header.Value)
		}
		// m.Header["Micro-Topic"] = msg.Topic // only for RPC server, it somehow inspect Header for topic
		if _, ok := m.Header["Content-Type"]; !ok {
			m.Header["Content-Type"] = "application/json" // default to json codec
		}
		m.Partition = msg.Partition

		err := h.handler(h.ctx, p)
		if err == nil && h.subopts.AutoAck {
			sess.MarkMessage(msg, "")
		} else if err != nil {
			p.err = err
			if eh != nil {
				eh(h.ctx, p)
			} else {
				// log.Errorf("[kafka]: subscriber error: %v", err)
				fmt.Printf("[kafka] subscriber error: %v\n", err)
			}
		}
	}
	return nil
}
