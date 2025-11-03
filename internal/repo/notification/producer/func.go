package producer

import (
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	instance *NotificationProducer
	once     sync.Once
	mu       sync.RWMutex
)

func GetInstance() *NotificationProducer {
	once.Do(func() {
		instance = NewNotificationProducer()
	})
	return instance
}

func StartGlobalProducer() error {
	producer := GetInstance()
	return producer.Start()
}

func StopGlobalProducer() error {
	mu.Lock()
	defer mu.Unlock()

	if instance != nil {
		instance.Stop()
		instance = nil
		// Reset once to allow recreation
		once = sync.Once{}
	}
	return nil
}

func GetGlobalMetrics() map[string]interface{} {
	mu.RLock()
	defer mu.RUnlock()

	if instance != nil && instance.IsRunning {
		return instance.GetMetrics()
	}
	return map[string]interface{}{
		"status": "not_running",
	}
}

// Graceful restart
func RestartGlobalProducer() error {
	if err := StopGlobalProducer(); err != nil {
		logrus.WithError(err).Error("Failed to stop producer")
		return err
	}

	return StartGlobalProducer()
}
