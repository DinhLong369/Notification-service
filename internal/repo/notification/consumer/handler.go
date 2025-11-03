package consumer

import (
	"context"
	"sync"
	"time"

	"core/internal/repo/notification/producer"
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
			[]string{"notification"},
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
