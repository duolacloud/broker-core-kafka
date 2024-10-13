package main

import (
	"context"
	"runtime"

	"log"

	"github.com/duolacloud/broker-core"
	kafka "github.com/duolacloud/broker-core-kafka"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	bopts := make([]broker.Option, 0)
	bopts = append(bopts, broker.Addrs("xx:9198", "xx:9198"))
	bopts = append(bopts, kafka.WithConsumerGroup("test_group"))
	bopts = append(bopts, kafka.WithAccessKey("xx"))
	bopts = append(bopts, kafka.WithSecretKey("I/xx"))
	bopts = append(bopts, kafka.WithRegionKey("ap-east-1"))
	b := kafka.NewBroker(bopts...)
	if err := b.Connect(); err != nil {
		log.Fatal("broker connect failed", err)
	}

	err := b.Publish(context.Background(), "test_topic", &broker.Message{
		Header: map[string]string{
			"key": "value",
		},
		Body:      []byte("hello world"),
		Partition: 0,
	})
	if err == nil {
		log.Printf("publish success\n")
	}
	b.Subscribe("test_topic", func(ctx context.Context, event broker.Event) error {
		if err != nil {
			log.Fatal("subscribe failed", err)
			return nil
		}
		log.Printf("subscribe success, message: %s\n", string(event.Message().Body))
		return nil
	})
	runtime.Gosched()
	select {}
}

/*
发送和消费消息之前需要先创建topic和分区
func CreateTopic(topic string, numPartitions int32, replicationFactor int16) error {
	// 创建 TopicDetails
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     map[string]*string{},
	}

	// 使用 Admin 创建 topic
	admin, err := sarama.NewClusterAdmin(k.addrs, k.getBrokerConfig())
	if err != nil {
		return err
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return err
	}
	return nil
}
*/
