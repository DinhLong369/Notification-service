package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// NewProducer khởi tạo Producer mới
func NewProducer() *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(KafkaConfig.Brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *Producer) Send(topic string, key string, v any) error {
	b, _ := json.Marshal(v)
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Topic: topic, Key: []byte(key), Value: b,
	})
}

func (p *Producer) Close() error { return p.writer.Close() }
