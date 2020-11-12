package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	var (
		topic   = "test"
		brokers = []string{"localhost:9092"}
	)
	w := kafka.NewWriter(kafka.WriterConfig{
		Topic:        topic,
		Brokers:      brokers,
		Logger:       kafka.LoggerFunc(log.Printf),
		BatchSize:    10000,
		RequiredAcks: 1,
	})

	// "Create a topic with 3 partitions and fill it with 10_000 raws"
	// $ kaf topic create -r 1 -p 3 test
	// ✅ Created topic!
	// 	  Topic Name:            test
	// 	  Partitions:            3
	// 	  Replication Factor:    1
	// 	  Cleanup Policy:        delete
	ctx := context.Background()
	messages := make([]kafka.Message, 10000)
	for i := 0; i < 10000; i++ {
		value := strconv.Itoa(i)
		messages[i] = kafka.Message{Key: []byte(value), Value: []byte(value)}
	}
	start := time.Now()
	if err := w.WriteMessages(ctx, messages...); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", time.Since(start))

	// Later create a consumer passing a consumer group id
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers, // just one bbroker
		Topic:       topic,
		GroupID:     "worker",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     10 * time.Second,
		Logger:      kafka.LoggerFunc(log.Printf),
		StartOffset: kafka.FirstOffset, // also I changed this but the same result
	})

	// Then try to read the topic
	c := 0
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d) Key: %s Value: %s Parition: %d Offset: %d\n", c, msg.Key, msg.Value, msg.Partition, msg.Offset)
		c++
	}

}
