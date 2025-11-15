package notification

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

type Query struct {
	Id     uuid.UUID `query:"id"`
	Limit  int       `query:"limit"`
	Page   int       `query:"page"`
	System string    `query:"system"`
}

func (query *Query) Parse(c *fiber.Ctx) {
	if err := c.QueryParser(query); err != nil {
		query.Limit = 10
		query.Page = 1
	}
	if query.Page <= 0 {
		query.Page = 1
	}
	if query.Limit <= 0 {
		query.Limit = 10
	}
}
