package middleware

import (
	"os"

	"github.com/gofiber/fiber/v2"
)

func APIKeyAuth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		apiKey := c.Get("X-API-KEY")
		expectedAPIKey := os.Getenv("API_KEY")

		if apiKey == "" || apiKey != expectedAPIKey {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"status": false, "error": "unauthorized"})
		}

		return c.Next()
	}
}
