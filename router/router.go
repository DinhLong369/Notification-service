package router

import (
	mainapp "core/app"
	handler "core/internal/handler"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

func Setup() {
	app := fiber.New(fiber.Config{})
	app.Use(cors.New())
	app.Use(recover.New())
	setupRouter(app)
	port := mainapp.Config("WEB_PORT")
	if len(port) == 0 {
		port = "3636"
	}
	fmt.Println("port=", port)
	app.Listen(":" + port)
}

func setupRouter(fiber_app *fiber.App) {
	api := fiber_app.Group("/api", logger.New())

	api.Get("/test.json", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": true, "message": "Pong"})
	})

	// Notifications
	api.Post("/notifications", handler.CreateNotification)

	api.Get("/notifications", handler.ListNotifications)

	api.Post("/device_token", handler.CreateTokenDevice)
}
