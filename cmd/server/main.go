package main

import (
	"core/app"
	"core/internal/repo/notification/consumer"
	"core/internal/repo/notification/outbox_pattern"
	"core/internal/repo/notification/producer"
	"core/lib/kafka"
	"core/router"
	"fmt"
)

const NOTIFICATION_TOPIC = "notification"

func main() {
	app.Setup()
	fmt.Println("*************** SETUP KAFKA ***************")
	kafka.Setup()

	// Tạo topic cho notification
	if err := kafka.CreateTopic(NOTIFICATION_TOPIC, 3, 1); err != nil {
		fmt.Printf("Failed to create notification topic: %v\n", err)
	} else {
		fmt.Println("Notification topic created successfully")
	}

	// Start notification producer
	if err := producer.StartGlobalProducer(); err != nil {
		fmt.Printf("Failed to start notification producer: %v\n", err)
	} else {
		fmt.Println("Notification producer started successfully")
	}

	// Start notification consumer
	processor := &consumer.NotificationProcessor{}
	processor.Init()
	fmt.Println("Notification consumer started successfully")

	// Start outbox worker để xử lý các events bị failed
	outboxWorker := outbox_pattern.NewOutboxWorker()
	outboxWorker.Start()

	router.Setup()
}
