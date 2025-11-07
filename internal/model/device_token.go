package model

// UserDeviceToken lưu thông tin token của từng thiết bị/trình duyệt của user
type DeviceToken struct {
	Model
	UserID      string `json:"user_id"`
	DeviceToken string `json:"device_token" gorm:"uniqueIndex;not null"`
	DeviceType  string `json:"device_type" gorm:"not null"` // mobile, web, ...
	Expired     bool   `json:"expired" gorm:"default:false"`
	System      string `json:"system"`
}
