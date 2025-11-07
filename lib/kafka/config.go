package kafka

import (
	"context"
	"core/app"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers []string
	GroupID string
}

var KafkaConfig *Config

func Setup() {
	KafkaConfig = &Config{
		Brokers: strings.Split(app.Config("KAFKA_BROKERS"), ","),
		GroupID: app.Config("KAFKA_GROUP_ID"),
	}

	// Tạo topic test kết nối nếu chưa có
	topic := "test_connection"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", KafkaConfig.Brokers[0])
	if err == nil {
		_ = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})

		conn.Close()
		fmt.Println("Đã kết nối Kafka thành công!")
	} else {
		fmt.Println("DISABLE KAFKA!")
	}
}
