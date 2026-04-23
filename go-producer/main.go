package main

import (
    "context"
    "encoding/json"
    "flag"
    "log"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "syscall"
    "time"

    "github.com/IBM/sarama"
)

// Message структура для отправки структурированных данных
type Message struct {
    ID        string    `json:"id"`
    Timestamp time.Time `json:"timestamp"`
    Data      string    `json:"data"`
    Source    string    `json:"source"`
}

// ProducerConfig конфигурация продюсера
type ProducerConfig struct {
    Brokers     string
    Topic       string
    Messages    int
    Delay       time.Duration
    BatchSize   int
    Structured  bool
}

func main() {
    // Парсинг аргументов командной строки
    config := parseFlags()
    
    // Настройка логгера
    setupLogger()
    
    // Создание продюсера
    producer, err := createProducer(strings.Split(config.Brokers, ","))
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    defer producer.Close()
    
    // Обработка сигналов graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    setupSignalHandler(cancel)
    
    // Запуск отправки сообщений
    runProducer(ctx, producer, config)
}

func parseFlags() *ProducerConfig {
    brokers := flag.String("brokers", "localhost:9092", "Kafka brokers (comma-separated)")
    topic := flag.String("topic", "test-topic", "Kafka topic")
    messages := flag.Int("messages", 10, "Number of messages to send")
    delay := flag.Int("delay", 1, "Delay between messages in seconds")
    batchSize := flag.Int("batch", 1, "Batch size for sending messages")
    structured := flag.Bool("structured", true, "Send structured JSON messages")
    
    flag.Parse()
    
    return &ProducerConfig{
        Brokers:    *brokers,
        Topic:      *topic,
        Messages:   *messages,
        Delay:      time.Duration(*delay) * time.Second,
        BatchSize:  *batchSize,
        Structured: *structured,
    }
}

func setupLogger() {
    log.SetOutput(os.Stdout)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

func setupSignalHandler(cancel context.CancelFunc) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    go func() {
        <-sigChan
        log.Println("Received shutdown signal...")
        cancel()
    }()
}

func createProducer(brokers []string) (sarama.SyncProducer, error) {
    config := sarama.NewConfig()
    
    // Настройки производительности
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Retry.Max = 5
    config.Producer.Return.Successes = true
    config.Producer.Compression = sarama.CompressionSnappy
    config.Producer.Flush.Frequency = 500 * time.Millisecond
    config.Producer.MaxMessageBytes = 1048576 // 1MB
    
    // Настройки таймаутов
    config.Producer.Timeout = 10 * time.Second
    
    // Настройки сети
    config.Net.MaxOpenRequests = 5
    config.Net.DialTimeout = 30 * time.Second
    config.Net.ReadTimeout = 30 * time.Second
    config.Net.WriteTimeout = 30 * time.Second
    
    // Версия Kafka
    config.Version = sarama.V2_8_0_0
    
    return sarama.NewSyncProducer(brokers, config)
}

func runProducer(ctx context.Context, producer sarama.SyncProducer, cfg *ProducerConfig) {
    log.Printf("Starting producer: topic=%s, messages=%d, delay=%v, batch=%d",
        cfg.Topic, cfg.Messages, cfg.Delay, cfg.BatchSize)
    
    var messageCount int
    ticker := time.NewTicker(cfg.Delay)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            log.Printf("Producer stopped. Total messages sent: %d", messageCount)
            return
        case <-ticker.C:
            if messageCount >= cfg.Messages {
                log.Println("All messages sent successfully")
                return
            }
            
            // Создание и отправка сообщений
            for i := 0; i < cfg.BatchSize && messageCount < cfg.Messages; i++ {
                messageCount++
                if err := sendMessage(producer, cfg.Topic, messageCount, cfg.Structured); err != nil {
                    log.Printf("Failed to send message %d: %v", messageCount, err)
                } else {
                    log.Printf("✓ Sent message %d/%d", messageCount, cfg.Messages)
                }
            }
        }
    }
}

func sendMessage(producer sarama.SyncProducer, topic string, seq int, structured bool) error {
    var value sarama.Encoder
    
    if structured {
        msg := Message{
            ID:        strconv.Itoa(seq),
            Timestamp: time.Now(),
            Data:      "Message from Go #" + strconv.Itoa(seq),
            Source:    "go-producer",
        }
        
        jsonData, err := json.Marshal(msg)
        if err != nil {
            return err
        }
        value = sarama.ByteEncoder(jsonData)
    } else {
        value = sarama.StringEncoder("Message from Go #" + strconv.Itoa(seq))
    }
    
    producerMsg := &sarama.ProducerMessage{
        Topic: topic,
        Value: value,
        Key:   sarama.StringEncoder(strconv.Itoa(seq)),
        Headers: []sarama.RecordHeader{
            {
                Key:   []byte("timestamp"),
                Value: []byte(time.Now().Format(time.RFC3339)),
            },
            {
                Key:   []byte("source"),
                Value: []byte("go-producer"),
            },
        },
    }
    
    partition, offset, err := producer.SendMessage(producerMsg)
    if err == nil {
        log.Printf("  → Partition: %d, Offset: %d", partition, offset)
    }
    
    return err
}
