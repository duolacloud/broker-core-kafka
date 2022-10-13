package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/duolacloud/broker-core"
	"github.com/stretchr/testify/assert"
)

func TestKafkaAsyncPub(t *testing.T) {
	errorsChan := make(chan *sarama.ProducerError)
	successesChan := make(chan *sarama.ProducerMessage)
	defer close(successesChan)
	defer close(errorsChan)

	b := NewBroker(AsyncProducer(errorsChan, successesChan))
	err := b.Connect()
	assert.Nil(t, err)

	err = b.Publish("test", &broker.Message{
		Body: []byte("hello"),
	})
	assert.Nil(t, err)
	time.Sleep(3 * time.Second)
}

func TestKafkaSyncPub(t *testing.T) {
	b := NewBroker()
	err := b.Connect()
	assert.Nil(t, err)

	err = b.Publish("test", &broker.Message{
		Body: []byte("world"),
	})
	assert.Nil(t, err)
}
