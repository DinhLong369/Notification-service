package model

import (
	"gorm.io/datatypes"
)

type Notification struct {
	Model       `gorm:"embedded"`
	Title       string         `json:"title"`
	Content     string         `gorm:"type:text;default:null" json:"content,omitempty"`
	To          datatypes.JSON `json:"to"`
	From        datatypes.JSON `json:"from"`
	Metadata    datatypes.JSON `json:"metadata"`
	IsRead      bool           `json:"is_read" gorm:"default:false"`
	TokenDevice string         `json:"token_device"`
}
