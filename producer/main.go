package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	bootstrapServers = "127.0.0.1:9092"
	acks             = "all"
	topic            = "purchases"
	messageCount     = 10
	flushTimeout     = 15 * time.Second
)

var (
	users = []string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	items = []string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              acks,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer p.Close()

	go reportDeliveries(p.Events())

	t := topic
	for range messageCount {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &t, Partition: kafka.PartitionAny},
			Key:            []byte(randomItem(users)),
			Value:          []byte(randomItem(items)),
		}
		if err := p.Produce(msg, nil); err != nil {
			return fmt.Errorf("failed to produce message: %w", err)
		}
	}

	if remaining := p.Flush(int(flushTimeout.Milliseconds())); remaining > 0 {
		return fmt.Errorf("%d message(s) not delivered within %s", remaining, flushTimeout)
	}
	return nil
}

func reportDeliveries(events <-chan kafka.Event) {
	for e := range events {
		ev, ok := e.(*kafka.Message)
		if !ok {
			continue
		}
		if ev.TopicPartition.Error != nil {
			fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			continue
		}
		fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
			*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
	}
}

func randomItem(list []string) string {
	return list[rand.Intn(len(list))]
}
