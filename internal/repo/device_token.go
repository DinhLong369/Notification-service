package notification

import (
	"core/app"
	"core/internal/model"
	"encoding/json"
	"fmt"

	"gorm.io/datatypes"
)

func CreateTokenDevice(entry model.DeviceToken) error {
	return app.Database.DB.Create(&entry).Error
}

// GetDeviceTokensByUserID trả về danh sách device token còn hiệu lực theo user_id
func GetDeviceTokensByUserID(userID string) ([]string, error) {
	var tokens []string
	var deviceTokens []model.DeviceToken
	err := app.Database.DB.Where("user_id = ?", userID).Find(&deviceTokens).Error
	if err != nil {
		return nil, err
	}
	for _, dt := range deviceTokens {
		tokens = append(tokens, dt.DeviceToken)
	}
	return tokens, nil
}

func ExtractUserIDs(to datatypes.JSON) ([]string, error) {
	var ids []string

	// Thử parse dạng array trước
	var arr []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(to, &arr); err == nil {
		for _, u := range arr {
			ids = append(ids, u.ID)
		}
		return ids, nil
	}

	// Nếu không phải array, thử parse dạng object
	var obj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(to, &obj); err == nil && obj.ID != "" {
		return []string{obj.ID}, nil
	}

	return nil, fmt.Errorf("invalid JSON in 'To'")
}
