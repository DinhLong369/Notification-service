package outbox_pattern

import (
	"context"
	"time"

	"core/app"
	"core/internal/model"

	"github.com/google/uuid"
)

func CreateOutboxEvent(event *model.OutboxEvent) error {
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}

	ctx, cancel := context.WithTimeout(context.Background(), app.DBTimeout)
	defer cancel()

	return app.Database.DB.WithContext(ctx).Create(event).Error
}

func (w *OutboxWorker) getPendingOutboxEvents() ([]model.OutboxEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), app.DBTimeout)
	defer cancel()

	var events []model.OutboxEvent

	// Query với điều kiện:
	err := app.Database.DB.WithContext(ctx).
		Where("status = ? AND retry_count < ?", "pending", w.maxRetries).
		Where(
			// Điều kiện OR:
			// 1. Hoặc là event mới (retry_count = 0)
			// 2. Hoặc là event đã đến lúc retry (NOW() >= processed_at + backoff_interval)
			"retry_count = 0 OR NOW() >= DATE_ADD(processed_at, INTERVAL POWER(2, retry_count - 1) * ? SECOND)",
			w.baseRetryDelay.Seconds(),
		).
		Order("created_at ASC").
		Limit(w.batchSize).
		Find(&events).Error

	return events, err
}

