package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"core/internal/model"
	"core/internal/repo/notification/outbox_pattern"
	"core/lib/kafka"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/datatypes"
)

const (
	FLUSH_INTERVAL = 1 * time.Second
	MAX_BATCH_SIZE = 100
	CHANNEL_BUFFER = 10000
	WORKER_COUNT   = 3
)

type NotificationData struct {
	ID         uuid.UUID      `json:"id,omitempty"`
	CreatedAt  *time.Time     `json:"created_at,omitempty"`
	UpdatedAt  *time.Time     `json:"updated_at,omitempty"`
	Title      string         `json:"title"`
	Content    string         `json:"content,omitempty"`
	To         datatypes.JSON `json:"to"`
	From       datatypes.JSON `json:"from"`
	Metadata   datatypes.JSON `json:"metadata"`
	IsRead     bool           `json:"is_read"`
	Timestamp  time.Time      `json:"timestamp"`
	RetryCount int            `json:"retry_count"`
	System     string         `json:"system"`
}

type NotificationProducer struct {
	sync.RWMutex        // Lock để protect shared data
	IsRunning    bool   // Trạng thái running
	Topic        string // Kafka topic name

	// Channel-based architecture - Kiến trúc dựa trên channels
	inputChannel chan NotificationData            // Channel nhận data từ API
	flushChannel chan map[string]NotificationData // Channel truyền batch data
	buffer       map[string]NotificationData      // Buffer chính để merge data

	FlushInterval time.Duration // Cấu hình flush interval
	BatchSize     int           // Cấu hình batch size

	// Performance metrics - Các chỉ số performance (atomic counters)
	StartTime      time.Time // Thời gian bắt đầu
	ProcessedItems int64     // Số items đã xử lý thành công
	BufferSize     int64     // Kích thước buffer hiện tại
	ErrorCount     int64     // Số lỗi đã xảy ra
	DroppedCount   int64     // Số items bị drop
	LastError      string    // Lỗi cuối cùng
	Status         string    // Trạng thái hiện tại

	// Context for lifecycle management - Quản lý lifecycle
	ctx     context.Context    // Context để signal shutdown
	cancel  context.CancelFunc // Function để cancel context
	workers sync.WaitGroup     // WaitGroup để đợi workers finish
}

// Set default values cho intervals và status
func NewNotificationProducer() *NotificationProducer {
	return &NotificationProducer{
		Topic:         "notification_service",                      // Kafka topic
		inputChannel:  make(chan NotificationData, CHANNEL_BUFFER), // Channel với buffer 10k
		flushChannel:  make(chan map[string]NotificationData, 10),  // Channel nhỏ hơn
		buffer:        make(map[string]NotificationData),           // Buffer map
		FlushInterval: FLUSH_INTERVAL,                              // 1 second
		BatchSize:     MAX_BATCH_SIZE,                              // 100 items
		Status:        "stopped",                                   // Initial status
	}
}

func (p *NotificationProducer) Start() error {
	p.Lock()
	defer p.Unlock()

	if p.IsRunning {
		return fmt.Errorf("producer is already running") // Tránh start nhiều lần
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.IsRunning = true
	p.StartTime = time.Now()
	p.Status = "running"
	// Reset tất cả counters về 0
	atomic.StoreInt64(&p.ErrorCount, 0)
	atomic.StoreInt64(&p.ProcessedItems, 0)
	atomic.StoreInt64(&p.DroppedCount, 0)
	// Start 3 input workers để xử lý data từ channel
	for i := 0; i < WORKER_COUNT; i++ {
		p.workers.Add(1)
		go p.inputWorker(i)
	}
	// Start batch processor để merge data từ workers
	p.workers.Add(1)
	go p.batchProcessor()
	logrus.WithField("topic", p.Topic).Info("Notification producer started with channel-based architecture")
	return nil
}

// Tầng xử lí đầu tiên : Input Workers
func (p *NotificationProducer) inputWorker(workerID int) {
	defer p.workers.Done() // Signal worker finished khi exit

	// Mỗi worker có local buffer riêng để giảm contention
	localBuffer := make(map[string]NotificationData, p.BatchSize) // p.BatchSize = 100
	ticker := time.NewTicker(100 * time.Millisecond)              // Flush định kỳ mỗi 100ms
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done(): // Nhận signal shutdown
			// Flush remaining data trước khi exit
			if len(localBuffer) > 0 {
				p.sendToFlushChannel(localBuffer)
			}
			logrus.WithField("worker_id", workerID).Debug("Input worker stopped")
			return
		case data := <-p.inputChannel: // Nhận data từ api request
			// Tạo unique key để merge duplicates
			key := data.ID.String()
			// Merge duplicate entries - chỉ giữ bản mới nhất
			if existing, exists := localBuffer[key]; exists {
				if data.Timestamp.After(existing.Timestamp) {
					localBuffer[key] = data // Ghi đè với data mới hơn
				}
				// Nếu data cũ hơn thì bỏ qua (không ghi đè)
			} else {
				localBuffer[key] = data // Data mới, add vào buffer
			}
			// Mini-batch flush khi đạt batch size
			// Chia đều cho 3 workers : 100/3 ≈ 33 items per worker
			if len(localBuffer) >= p.BatchSize/WORKER_COUNT {
				p.sendToFlushChannel(localBuffer)
				localBuffer = make(map[string]NotificationData, p.BatchSize)
			}
		case <-ticker.C: // Flush định kỳ mỗi 100ms
			if len(localBuffer) > 0 {
				p.sendToFlushChannel(localBuffer)
				localBuffer = make(map[string]NotificationData, p.BatchSize)
			}
		}
	}
}

// Batch Processor - Tầng merge chính
func (p *NotificationProducer) batchProcessor() {
	defer p.workers.Done()                    // Signal worker finished khi exit
	ticker := time.NewTicker(p.FlushInterval) // 1 second
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done(): // Nhận signal shutdown
			// Final flush trước khi thoát
			if len(p.buffer) > 0 {
				p.flushToKafka()
			}
			logrus.Debug("Batch processor stopped")
			return
		case batch := <-p.flushChannel: // Nhận mini-batch từ input workers
			// Merge into main buffer - final deduplication
			for key, data := range batch {
				if existing, exists := p.buffer[key]; exists {
					// So sánh timestamp để giữ data mới nhất
					if data.Timestamp.After(existing.Timestamp) {
						p.buffer[key] = data
					}
				} else {
					p.buffer[key] = data
				}
			}
			// Update buffer size metric
			atomic.StoreInt64(&p.BufferSize, int64(len(p.buffer)))
			// Auto flush khi buffer đầy
			if len(p.buffer) >= p.BatchSize {
				p.flushToKafka()
			}
		case <-ticker.C: // Periodic flush mỗi 1 giây
			if len(p.buffer) > 0 {
				p.flushToKafka()
			}
		}
	}
}

func (p *NotificationProducer) setLastError(errMsg string) {
	p.RLock()
	defer p.RUnlock()
	p.LastError = errMsg
}

// Send to Flush Channel
func (p *NotificationProducer) sendToFlushChannel(batch map[string]NotificationData) {
	if len(batch) == 0 {
		return
	}

	// Copy data để tránh race conditions (tránh đọc ghi đồng thời)
	batchCopy := make(map[string]NotificationData, len(batch))
	for k, v := range batch {
		batchCopy[k] = v
	}

	select {
	case p.flushChannel <- batchCopy: // Non-blocking send
		// Success
	default:
		// Channel full - system overloaded
		atomic.AddInt64(&p.DroppedCount, int64(len(batchCopy)))
		logrus.Warn("Flush channel full, dropping batch")
	}
}

// Flush to Kafka - Tầng cuối cùng:
func (p *NotificationProducer) flushToKafka() {
	if len(p.buffer) == 0 {
		return
	}

	// Copy buffer để không block main flow
	toFlush := make(map[string]NotificationData, len(p.buffer))
	for k, v := range p.buffer {
		toFlush[k] = v
	}

	// Clear main buffer ngay lập tức để accept data mới
	p.buffer = make(map[string]NotificationData)
	atomic.StoreInt64(&p.BufferSize, 0)

	// Process trong background goroutine để không block
	go func(data map[string]NotificationData) {
		producer := kafka.NewProducer()
		defer producer.Close()
		successCount := 0

		for key, notificationData := range data {
			partitionKey := key // Sử dụng notification ID key để partition
			if err := producer.Send(p.Topic, partitionKey, notificationData); err != nil {
				logrus.WithError(err).WithField("key", key).Error("Failed to send to Kafka")

				// Retry logic
				notificationData.RetryCount++
				if notificationData.RetryCount < 3 { // Max 3 retries
					// Re-add vào input channel để retry
					select {
					case p.inputChannel <- notificationData:
						// Successfully re-queued
					default:
						// Channel full, count as dropped
						atomic.AddInt64(&p.DroppedCount, 1)
						logrus.WithField("key", key).Warn("Failed to re-queue for retry")
					}
				} else {
					// Max retries reached -> persist to outbox for later retry by worker
					atomic.AddInt64(&p.DroppedCount, 1)
					logrus.WithField("key", key).Warn("Max retries reached, persisting to outbox")

					// Marshal original notification data and save to outbox table
					eventData, err := json.Marshal(notificationData)
					if err != nil {
						logrus.WithError(err).WithField("key", key).Error("Failed to marshal notification for outbox")
						atomic.AddInt64(&p.ErrorCount, 1)
						p.setLastError(err.Error())
						continue
					}

					outboxEntry := model.OutboxEvent{
						ID:          notificationData.ID,
						AggregateID: notificationData.ID.String(),
						EventType:   "notification",
						EventData:   eventData,
						Status:      "pending",
					}
					if err := outbox_pattern.CreateOutboxEvent(&outboxEntry); err != nil {
						logrus.WithError(err).WithField("key", key).Error("Failed to persist notification to outbox")
						atomic.AddInt64(&p.ErrorCount, 1)
						p.setLastError(err.Error())
						continue
					}
				}

				atomic.AddInt64(&p.ErrorCount, 1)
				p.setLastError(err.Error())
				continue
			}
			successCount++
		}

		// Update metrics
		atomic.AddInt64(&p.ProcessedItems, int64(successCount))

		if successCount > 0 {
			logrus.WithFields(logrus.Fields{
				"topic":         p.Topic,
				"success_count": successCount,
				"total_batch":   len(data),
			}).Debug("Batch flushed to Kafka")
		}
	}(toFlush) // Pass copy của data vào goroutine
}

func (p *NotificationProducer) AddNotification(notificationData NotificationData) error {
	if !p.IsRunning {
		return fmt.Errorf("producer is not running")
	}

	// Set timestamp nếu chưa có
	if notificationData.Timestamp.IsZero() {
		notificationData.Timestamp = time.Now()
	}
	// Set ID nếu chưa có
	if notificationData.ID == uuid.Nil {
		notificationData.ID = uuid.New()
	}
	notificationData.RetryCount = 0 // Lần đầu gửi

	select {
	case p.inputChannel <- notificationData: // Đưa vào channel (Non-blocking send)
		return nil // Success - trả về ngay
	default:
		// Channel đầy, không thể queue thêm
		atomic.AddInt64(&p.DroppedCount, 1)
		return fmt.Errorf("input channel full, request dropped")
	}
}

// Stop - Graceful Shutdown:
func (p *NotificationProducer) Stop() {
	p.Lock()
	defer p.Unlock()

	if !p.IsRunning {
		return
	}

	logrus.Info("Stopping notification producer...")

	// Step 1: Cancel context để signal tất cả workers
	if p.cancel != nil {
		p.cancel()
	}

	// Step 2: Close input channel để không nhận thêm data
	close(p.inputChannel)

	// Step 3: Wait for all workers finish (với remaining data flush)
	p.workers.Wait()

	// Step 4: Close flush channel
	close(p.flushChannel)

	// Step 5: Reset state
	p.IsRunning = false
	p.Status = "stopped"
	p.ctx = nil
	p.cancel = nil

	logrus.Info("Notification producer stopped")
}

// Metrics & Monitoring:
func (p *NotificationProducer) GetMetrics() map[string]interface{} {
	p.RLock()
	defer p.RUnlock()

	return map[string]interface{}{
		"is_running":      p.IsRunning,                         // Trạng thái
		"status":          p.Status,                            // Status string
		"buffer_size":     atomic.LoadInt64(&p.BufferSize),     // Items trong buffer
		"channel_size":    len(p.inputChannel),                 // Items trong channel
		"processed_items": atomic.LoadInt64(&p.ProcessedItems), // Đã xử lý thành công
		"error_count":     atomic.LoadInt64(&p.ErrorCount),     // Số lỗi
		"dropped_count":   atomic.LoadInt64(&p.DroppedCount),   // Số items bị drop
		"last_error":      p.LastError,                         // Lỗi cuối cùng
		"uptime_seconds":  time.Since(p.StartTime).Seconds(),   // Thời gian chạy
	}
}
