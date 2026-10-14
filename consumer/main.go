package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	bootstrapServers = "127.0.0.1:9092"
	groupID          = "kafka-go-getting-started"
	autoOffsetReset  = "earliest"
	topic            = "purchases"
	pollTimeout      = 100 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic %q: %w", topic, err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consume(c, sigchan)
	return nil
}

func consume(c *kafka.Consumer, sigchan <-chan os.Signal) {
	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			return
		default:
			ev, err := c.ReadMessage(pollTimeout)
			if err != nil {
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}
}
