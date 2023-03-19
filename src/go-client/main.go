package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func getEnv(key, defaultVal string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	strVal := getEnv(key, "")
	if strVal == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(strVal)
	if err != nil {
		panic(err)
	}
	return val
}

// see: https://docs.redpanda.com/docs/get-started/code-examples/
func createTopic(brokers []string, topic string, topicPartition int, topicReplication int) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	ctx := context.Background()

	metadata, err := admin.Metadata(context.Background())
	if err != nil {
		panic(err)
	}
	for _, t := range metadata.Topics {
		if t.Topic == topic {
			fmt.Printf("Topic already exists: %s", topic)
			return
		}
	}

	// create topic
	resp, err := admin.CreateTopics(ctx, int32(topicPartition), int16(topicReplication), nil, topic)
	if err != nil {
		panic(err)
	}
	for _, ctr := range resp {
		if ctr.Err != nil {
			fmt.Printf("Unable to create topic '%s': %s", ctr.Topic, ctr.Err)
		} else {
			fmt.Printf("Created topic '%s'", ctr.Topic)
		}
	}
}

func consume(brokers []string, topic, consumerGroup string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	for {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// All errors are retried internally when fetching, but non-retriable
			// errors are returned from polls so that users can notice and take
			// action.
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			topicInfo := fmt.Sprintf("topic: %s (%d|%d)",
				record.Topic, record.Partition, record.Offset)
			messageInfo := fmt.Sprintf("key: %s, Value: %s",
				record.Key, record.Value)
			fmt.Printf("Message consumed: %s, %s \n", topicInfo, messageInfo)
		}
	}
}

func produce(brokers []string, topic, msgKey, msgVal string, repeat int) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	for i := 0; i < repeat; i++ {
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(msgKey),
			Value: []byte(fmt.Sprintf("%s %d", msgVal, i)),
		}
		results := client.ProduceSync(ctx, record)
		for _, pr := range results {
			if pr.Err != nil {
				fmt.Printf("Error sending synchronous message: %v \n", pr.Err)
			} else {
				fmt.Printf("Message sent: topic: %s, offset: %d, value: %s \n",
					topic, pr.Record.Offset, pr.Record.Value)
			}
		}
	}
}

func main() {
	brokers := strings.Split(getEnv("APP_KAFKA_BROKER", "localhost:19092"), ";")
	topic := getEnv("APP_KAFKA_TOPIC", "topic")
	topicPartition := getIntEnv("APP_KAFKA_TOPIC_PARTITION", 3)
	topicReplication := getIntEnv("APP_KAFKA_TOPIC_REPLICATION", 2)
	createTopic(brokers, topic, topicPartition, topicReplication)
	is_consumer := getEnv("APP_MODE", "consumer") == "consumer"
	if is_consumer {
		// consumer
		consumer_group := getEnv("APP_CONSUMER_GROUP", "default")
		consume(brokers, topic, consumer_group)
		return
	}
	// producer
	msgKey := getEnv("APP_MESSAGE_KEY", "default")
	msgVal := getEnv("APP_MESSAGE_VALUE", "message")
	repeat := getIntEnv("APP_MESSAGE_REPEAT", 5)
	produce(brokers, topic, msgKey, msgVal, repeat)
}
