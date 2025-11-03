package kafka

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

func CreateTopic(topic string, partitions int, replicationFactor int) error {
	if KafkaConfig == nil || len(KafkaConfig.Brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is not set")
	}

	conn, err := kafka.Dial("tcp", KafkaConfig.Brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
}
