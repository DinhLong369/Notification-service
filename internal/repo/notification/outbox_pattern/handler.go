package outbox_pattern

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"core/app"
	"core/internal/model"
	"core/lib/kafka"

	"github.com/sirupsen/logrus"
)

type OutboxWorker struct {
	interval       time.Duration // Khoảng thời gian check outbox
	batchSize      int           // Số lượng events xử lý mỗi lần
	maxRetries     int           // Số lần retry tối đa
	baseRetryDelay time.Duration // Thời gian delay cơ bản
	isRunning      bool          // Trạng thái woker có đang chạy không
	stopCh         chan struct{} // Channel để dừng worker
}

func NewOutboxWorker() *OutboxWorker {
	return &OutboxWorker{
		interval:       10 * time.Second,       // Check mỗi 10s
		batchSize:      50,                     // Xử lí 50 msg 1 lần
		maxRetries:     5,                      // Retry tối đa 5 lần
		baseRetryDelay: 500 * time.Millisecond, // Delay cơ bản 500ms
		stopCh:         make(chan struct{}),    // Tạo channel để stop worker
	}
}

func (w *OutboxWorker) Start() {
	if w.isRunning {
		logrus.Warn("Outbox worker is already running")
		return // Nếu worker đang chạy thì không làm gì
	}

	w.isRunning = true
	logrus.Info("Starting outbox worker...")
	go w.processLoop() // Chạy processLoop trong goroutine riêng
}

func (w *OutboxWorker) processLoop() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Xử lý outbox
			w.processOutboxEvents() // Xử lý outbox events
		case <-w.stopCh:
			logrus.Info("Stopping outbox worker...")
			return // Dừng worker
		}
	}
}

func (w *OutboxWorker) processOutboxEvents() {
	// Lấy batch events từ outbox
	events, err := w.getPendingOutboxEvents()
	if err != nil {
		logrus.Errorf("Failed to fetch outbox events: %v", err)
		return
	}
	if len(events) == 0 {
		return
	}
	for _, event := range events {
		var processingErr error
		switch event.EventType {
		case "notification":
			processingErr = w.handleNotificationUpdate(event)
		default:
			processingErr = fmt.Errorf("unknown event type: %s", event.EventType)
			logrus.Warn(processingErr)
		}
		if processingErr != nil {
			w.markEventAsFailed(event, processingErr)
		} else {
			w.markEventAsProcessed(event)
		}
	}
}

func (w *OutboxWorker) handleNotificationUpdate(event model.OutboxEvent) error {
	var entry model.Notification
	if err := json.Unmarshal(event.EventData, &entry); err != nil {
		return fmt.Errorf("failed to unmarshal event data for event ID %s: %w", event.ID, err)
	}

	// Thử publish lại vào Kafka (thay vì ghi vào DB). Nếu thành công -> mark processed
	producer := kafka.NewProducer()
	defer producer.Close()

	if err := producer.Send("notification", entry.ID.String(), entry); err != nil {
		logrus.WithError(err).WithField("event_id", event.ID).Warn("Retrying outbox event failed to publish to Kafka")
		return err // Trả về lỗi để `markEventAsFailed` xử lý
	}

	logrus.Infof("Successfully republished outbox event %s to kafka.", event.ID)
	return nil
}

// Cập nhật trạng thái event là true
func (w *OutboxWorker) markEventAsProcessed(event model.OutboxEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), app.DBTimeout)
	defer cancel()
	now := time.Now()
	updates := map[string]interface{}{
		"status":       "processed",
		"processed_at": &now,
		"last_error":   nil, // Xóa lỗi cuối cùng khi xử lý thành công
	}

	if err := app.Database.DB.WithContext(ctx).Model(&event).Updates(updates).Error; err != nil {
		logrus.WithError(err).WithField("event_id", event.ID).Error("Failed to mark event as processed")
	}
}

// Cập nhật trạng thái event là failed, tăng retry_count và ghi lỗi
func (w *OutboxWorker) markEventAsFailed(event model.OutboxEvent, processingErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), app.DBTimeout)
	defer cancel()

	newRetryCount := event.RetryCount + 1
	status := "pending"

	// Nếu vượt quá số lần thử lại tối đa, đánh dấu là 'failed'
	if newRetryCount >= w.maxRetries {
		status = "failed"
		logrus.WithField("event_id", event.ID).Errorf("Event failed permanently after %d retries: %v", w.maxRetries, processingErr)
	}

	now := time.Now()
	errorString := processingErr.Error()
	updates := map[string]interface{}{
		"status":       status,
		"retry_count":  newRetryCount,
		"last_error":   &errorString,
		"processed_at": &now, // Cập nhật thời gian của lần thử lại cuối cùng
	}

	if err := app.Database.DB.WithContext(ctx).Model(&event).Updates(updates).Error; err != nil {
		logrus.WithError(err).WithField("event_id", event.ID).Error("Failed to update event retry status")
	}
}
