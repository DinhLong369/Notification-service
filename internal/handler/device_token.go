package handler

import (
	"core/internal/model"
	notification "core/internal/repo"

	"github.com/gofiber/fiber/v2"
)

func CreateTokenDevice(c *fiber.Ctx) error {
	var input struct {
		UserID      string `json:"user_id"`
		DeviceToken string `json:"device_token"`
		DeviceType  string `json:"device_type"`
		System      string `json:"system"`
	}
	if err := c.BodyParser(&input); err != nil {
		return c.JSON(fiber.Map{"status": false, "message": "Review your input"})
	}

	device_token := model.DeviceToken{
		UserID:      input.UserID,
		DeviceToken: input.DeviceToken,
		DeviceType:  input.DeviceType,
		System:      input.System,
	}
	if err := notification.CreateTokenDevice(device_token); err != nil {
		return c.JSON(fiber.Map{"status": false, "message": "Can not create token device", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": true, "message": "Create token device successfully"})
}
