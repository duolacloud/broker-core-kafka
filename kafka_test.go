package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/duolacloud/broker-core"
	"github.com/stretchr/testify/assert"
)

func TestAsyncPub(t *testing.T) {
	errorsChan := make(chan *sarama.ProducerError)
	successesChan := make(chan *sarama.ProducerMessage)
	defer close(successesChan)
	defer close(errorsChan)

	b := NewBroker(AsyncProducer(errorsChan, successesChan))
	err := b.Connect()
	assert.Nil(t, err)

	err = b.Publish("test", "hello")
	assert.Nil(t, err)
	time.Sleep(3 * time.Second)
}

func TestSyncPub(t *testing.T) {
	b := NewBroker()
	err := b.Connect()
	assert.Nil(t, err)

	err = b.Publish("test", "hello async")
	assert.Nil(t, err)
}

func TestSubscribe(t *testing.T) {
	b := NewBroker()
	err := b.Connect()
	assert.Nil(t, err)

	ch := make(chan string)

	b.Subscribe("test", func(e broker.Event) error {
		ch <- string(e.Message().([]byte))
		return nil
	})

	msg := <-ch
	t.Log(msg)
}

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestSubscribeStruct(t *testing.T) {
	b1 := NewBroker()
	err := b1.Connect()
	assert.Nil(t, err)

	b2 := NewBroker()
	err = b2.Connect()
	assert.Nil(t, err)

	ch := make(chan *User)

	b1.Subscribe("test", func(e broker.Event) error {
		ch <- e.Message().(*User)
		return nil
	}, broker.ResultType(&User{}))

	time.Sleep(3 * time.Second)
	b2.Publish("test", &User{Name: "jack", Age: 21})
	b2.Publish("test", &User{Name: "rose", Age: 30})

	u1 := <-ch
	t.Logf("%+v", u1)
	assert.Equal(t, "jack", u1.Name)
	assert.Equal(t, 21, u1.Age)

	u2 := <-ch
	t.Logf("%+v", u2)
	assert.Equal(t, "rose", u2.Name)
	assert.Equal(t, 30, u2.Age)
}
