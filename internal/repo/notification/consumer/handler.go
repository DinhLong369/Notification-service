package consumer

import (
	"context"
	"sync"
	"time"

	"core/app"
	"core/internal/model"
	notification "core/internal/repo"
	"core/internal/repo/notification/producer"
	"core/lib/fcm"
	"core/lib/kafka"

	"github.com/sirupsen/logrus"
)

type NotificationProcessor struct {
	batch             map[string]producer.NotificationData
	mu                sync.Mutex
	lastFlush         time.Time
	batchSize         int
	flushInterval     time.Duration
	maxRetries        int
	baseRetryInterval time.Duration
}

func (n *NotificationProcessor) Init() {
	n.batch = make(map[string]producer.NotificationData)
	n.lastFlush = time.Now()
	n.batchSize = 50
	n.flushInterval = 1 * time.Second
	n.maxRetries = 3
	n.baseRetryInterval = 500 * time.Millisecond

	// Đăng kí consumer sử dụng kafka worker framework
	go func() {
		worker := kafka.NewWorker[producer.NotificationData](
			"notification-consumer-group",
			[]string{"notification_service"},
			3, // 3 partitions
			func(ctx context.Context, msg kafka.Message[producer.NotificationData]) error {
				return n.Process(ctx, msg.Value)
			},
		)
		defer worker.Close()

		_ = worker.Run(context.Background())
	}()

	go n.startFlushWorker()
}

// Nơi consumer gửi event vào batch
func (n *NotificationProcessor) Process(ctx context.Context, data producer.NotificationData) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Merge với existing data trong batch sử dụng map
	key := data.ID.String()
	if existing, exists := n.batch[key]; exists {
		if data.Timestamp.After(existing.Timestamp) {
			n.batch[key] = data
		}
	} else {
		// -> Thêm mới vào batch
		n.batch[key] = data
	}

	notif := model.Notification{
		Model: model.Model{
			ID:        data.ID,
			CreatedAt: data.CreatedAt,
			UpdatedAt: data.UpdatedAt,
		},
		Title:    data.Title,
		Content:  data.Content,
		To:       data.To,
		From:     data.From,
		Metadata: data.Metadata,
		IsRead:   data.IsRead,
	}

	db := app.Database.DB.WithContext(ctx)
	if err := db.Create(&notif).Error; err != nil {
		logrus.WithError(err).WithField("notification_id", notif.Model.ID).Error("Failed to create notification from Kafka to DB")
	}

	userID, err := notification.ExtractSingleUserID(data.To)
	if err != nil {
		logrus.WithError(err).Warn("Failed to extract userID from notification.To")
		return err
	}

	//  Lấy device tokens theo userID
	tokens, err := notification.GetDeviceTokensByUserID(userID)
	if err != nil {
		logrus.WithError(err).WithField("user_id", userID).
			Error("Failed to get device tokens for user")
		return err
	}
	// Gửi thông báo về thiết bị qua FCM nếu có token_device
	if app.FCM.Enabled && len(tokens) > 0 {
		fcm.FCM.SendNotificationMulti(tokens, data.Title, data.Content, nil, nil)
	}

	// Khi batch đầy -> flush ngay
	if len(n.batch) >= n.batchSize {

		return n.flushBatch()
	}
	return nil
}

func (n *NotificationProcessor) startFlushWorker() {
	// Worker chạy mỗi 3 giây để flush batch vào database
	ticker := time.NewTicker(n.flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.mu.Lock()
		if len(n.batch) > 0 && time.Since(n.lastFlush) >= n.flushInterval {
			if err := n.flushBatch(); err != nil {
				logrus.Errorf("Flush worker error: %v", err)
			}
		}
		n.mu.Unlock()
	}
}

func (n *NotificationProcessor) flushBatch() error {
	if len(n.batch) == 0 {
		return nil
	}

	// Add to in-memory cache (populated from Kafka topic)

	n.batch = make(map[string]producer.NotificationData)
	n.lastFlush = time.Now()
	logrus.Infof("Cached %d notifications from kafka", len(n.batch))
	return nil
}

// Note: consumer no longer persists notifications to DB; outbox is used by producer only
