package main

import (
	"context"
	"crypto/tls"
	"runtime"

	"log"

	"github.com/IBM/sarama"
	"github.com/duolacloud/broker-core"
	kafka "github.com/duolacloud/broker-core-kafka"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	bopts := make([]broker.Option, 0)
	bopts = append(bopts, broker.Addrs([]string{"xxxx.com:9098"}...))
	bopts = append(bopts, awsMskIAMConfig("ap-southeast-1"))
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
	} else {
		log.Printf("publish failed, %v", err)
	}

	bsopts := make([]broker.SubscribeOption, 0)
	bsopts = append(bsopts, broker.Queue("123"))
	b.Subscribe("test_topic", func(ctx context.Context, event broker.Event) error {
		if err != nil {
			log.Fatal("subscribe failed", err)
			return nil
		}
		log.Printf("subscribe success, message: %s\n", string(event.Message().Body))
		return nil
	}, bsopts...)
	runtime.Gosched()
	select {}
}

type MSKAccessTokenProvider struct {
	region string
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(context.TODO(), m.region)
	return &sarama.AccessToken{Token: token}, err
}

func awsMskIAMConfig(region string) broker.Option {
	// Set the SASL/OAUTHBEARER configuration
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{region: region}

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return kafka.BrokerConfig(config)
}
