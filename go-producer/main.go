package main

import (
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Конфигурация продюсера
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Подключаемся к Kafka
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	// Отправляем 10 сообщений
	for i := 1; i <= 10; i++ {
		message := "Message from Go #" + strconv.Itoa(i)
		msg := &sarama.ProducerMessage{
			Topic: "test-topic",
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("Failed to send message:", err)
		} else {
			log.Printf("Sent: %s | Partition: %d | Offset: %d\n", message, partition, offset)
		}
		time.Sleep(1 * time.Second)
	}
}
