package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Model struct {
	ID        uuid.UUID       `gorm:"primaryKey;default:(UUID())" json:"id,omitempty"`
	CreatedAt *time.Time      `json:"created_at,omitempty"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
	DeletedAt *gorm.DeletedAt `json:"deleted_at,omitempty"`
}

func (u *Model) BeforeCreate(tx *gorm.DB) (err error) {
	if u.ID == uuid.Nil {
		u.ID = uuid.New()
	}
	return
}
