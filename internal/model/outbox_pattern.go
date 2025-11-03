package model

import (
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	ID          uuid.UUID  `json:"id" gorm:"type:char(36);primaryKey"`
	AggregateID string     `json:"aggregate_id" gorm:"index"`
	EventType   string     `json:"event_type"`
	EventData   []byte     `json:"event_data" gorm:"type:json"`
	Status      string     `json:"status" gorm:"default:'pending'"` // pending, processed, failed
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at"`
	RetryCount  int        `json:"retry_count" gorm:"default:0"`
	LastError   *string    `json:"last_error"`
}

