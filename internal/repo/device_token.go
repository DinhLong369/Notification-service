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
	var users []struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(to, &users); err != nil {
		return nil, fmt.Errorf("invalid JSON in 'To': %v", err)
	}

	var userIDs []string
	for _, user := range users {
		if user.ID != "" {
			userIDs = append(userIDs, user.ID)
		}
	}

	return userIDs, nil
}
