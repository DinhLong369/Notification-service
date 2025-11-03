package notification

import (
	"context"
	"encoding/json"
	"time"

	"core/app"
	"core/internal/model"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// CreateNotificationWithOutbox creates a Notification and corresponding OutboxEvent
// in a single DB transaction. The provided context is used for DB timeout/cancellation.
func CreateNotificationWithOutbox(ctx context.Context, notif *model.Notification) error {
	return app.Database.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		notif.CreatedAt = &now

		if err := tx.Create(notif).Error; err != nil {
			logrus.WithError(err).Error("failed to create notification in tx")
			return err
		}

		eventData, err := json.Marshal(notif)
		if err != nil {
			logrus.WithError(err).Error("failed to marshal notification for outbox in tx")
			return err
		}

		outboxEntry := model.OutboxEvent{
			ID:          notif.ID,
			AggregateID: notif.ID.String(),
			EventType:   "notification",
			EventData:   eventData,
			Status:      "pending",
			CreatedAt:   now,
		}

		if err := tx.Create(&outboxEntry).Error; err != nil {
			logrus.WithError(err).Error("failed to create outbox event")
			return err
		}

		return nil
	})
}

// CreateNotification creates only the Notification record (no outbox entry).
func CreateNotification(ctx context.Context, notif *model.Notification) error {
	now := time.Now()
	notif.CreatedAt = &now
	if err := app.Database.DB.WithContext(ctx).Create(notif).Error; err != nil {
		logrus.WithError(err).Error("failed to create notification")
		return err
	}
	return nil
}
