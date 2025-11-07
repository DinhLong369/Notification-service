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

func ExtractSingleUserID(to datatypes.JSON) (string, error) {
	var user struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(to, &user); err != nil {
		return "", fmt.Errorf("invalid JSON in 'To': %v", err)
	}

	if user.ID == "" {
		return "", fmt.Errorf("missing user ID in 'To'")
	}

	return user.ID, nil
}
