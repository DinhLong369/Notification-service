package handler

import (
	"encoding/json"
	"time"

	"core/internal/model"
	notification "core/internal/repo"
	outboxrepo "core/internal/repo/notification/outbox_pattern"
	notifproducer "core/internal/repo/notification/producer"

	"github.com/gofiber/fiber/v2"
	"github.com/sirupsen/logrus"
	"gorm.io/datatypes"
)

type CreateNotificationRequest struct {
	Title       string         `json:"title"`
	Content     string         `json:"content,omitempty"`
	To          datatypes.JSON `json:"to"`
	From        datatypes.JSON `json:"from"`
	Metadata    datatypes.JSON `json:"metadata"`
	TokenDevice string         `json:"token_device"`
}

// It creates a Notification record and an OutboxEvent in a single DB transaction.
func CreateNotification(c *fiber.Ctx) error {
	var req CreateNotificationRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"status": false, "error": "invalid request body"})
	}

	if req.Title == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"status": false, "error": "title is required"})
	}

	notif := model.Notification{
		Title:    req.Title,
		Content:  req.Content,
		To:       req.To,
		From:     req.From,
		Metadata: req.Metadata,
	}

	prod := notifproducer.GetInstance()

	pd := notifproducer.NotificationData{
		ID:        notif.ID,
		CreatedAt: notif.CreatedAt,
		UpdatedAt: notif.UpdatedAt,
		Title:     notif.Title,
		Content:   notif.Content,
		To:        notif.To,
		From:      notif.From,
		Metadata:  notif.Metadata,
		IsRead:    notif.IsRead,
		Timestamp: time.Now(),
	}

	if err := prod.AddNotification(pd); err != nil {
		logrus.WithError(err).WithField("notification_id", notif.ID).Warn("producer AddNotification failed, falling back to outbox")

		// Marshal notification into JSON for outbox payload
		eventData, jerr := json.Marshal(&notif)
		if jerr != nil {
			logrus.WithError(jerr).Error("failed to marshal notification for outbox")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": false, "error": "failed to queue and marshal notification"})
		}

		outboxEntry := model.OutboxEvent{
			ID:          notif.ID,
			AggregateID: notif.ID.String(),
			EventType:   "notification",
			EventData:   eventData,
			Status:      "pending",
			CreatedAt:   time.Now(),
		}

		if err := outboxrepo.CreateOutboxEvent(&outboxEntry); err != nil {
			logrus.WithError(err).Error("failed to persist outbox event after AddNotification failure")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": false, "error": "failed to persist outbox event"})
		}
	}

	// Return created notification
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"status": true, "data": notif})
}

func ListNotifications(c *fiber.Ctx) error {
	var query notification.Query
	query.Parse(c)
	themes, total, err := notification.GetAllNotifications(query)
	if err != nil {
		return c.JSON(fiber.Map{"status": false, "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": true, "data": themes, "total": total})
}
