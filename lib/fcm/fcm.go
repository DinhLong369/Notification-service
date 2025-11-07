package fcm

import (
	"context"
	"fmt"
	"strings"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

type FCMClient struct {
	client *messaging.Client
}

var FCM *FCMClient

// Setup khởi tạo Firebase Cloud Messaging client
func Setup(credentialsPath string) error {
	opt := option.WithCredentialsFile(credentialsPath)
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		return fmt.Errorf("error initializing firebase app: %v", err)
	}

	messagingClient, err := app.Messaging(context.Background())
	if err != nil {
		return fmt.Errorf("error getting messaging client: %v", err)
	}

	FCM = &FCMClient{
		client: messagingClient,
	}

	logrus.Info("Firebase Cloud Messaging initialized successfully")
	return nil
}

// SendNotification gửi thông báo đến thiết bị qua FCM token
func (c *FCMClient) SendNotification(token string, title string, body string, data map[string]string) error {
	msg := &messaging.Message{
		Token: token,
		Notification: &messaging.Notification{
			Title: title,
			Body:  body,
		},
		Data: data,
	}
	_, err := c.client.Send(context.Background(), msg)
	return err
}

// SendNotificationMulti gửi thông báo đến nhiều thiết bị, tự động xử lý token hết hạn
func (c *FCMClient) SendNotificationMulti(tokens []string, title, body string, data map[string]string, expireFunc func(token string)) {
	for _, token := range tokens {
		err := c.SendNotification(token, title, body, data)
		if err != nil && err.Error() != "" && (strings.Contains(err.Error(), "registration token is not registered") ||
			strings.Contains(err.Error(), "SenderId mismatch")) {
			if expireFunc != nil {
				expireFunc(token)
			}
		}
	}
}
