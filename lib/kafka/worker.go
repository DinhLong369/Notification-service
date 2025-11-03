package kafka

import (
	"context"
	"encoding/json"

	"github.com/gofiber/fiber/v2/log"
	"github.com/segmentio/kafka-go"
)

type Message[T any] struct {
	Topic     string
	Value     T
	Headers   map[string]string
	Key       string
	Raw       kafka.Message
	SessionID string
}

type Handler[T any] func(ctx context.Context, msg Message[T]) error

type Worker[T any] struct {
	r         *kafka.Reader
	sem       chan struct{}
	unmarshal func([]byte, any) error
	handle    Handler[T]
}

func NewWorker[T any](group string, topics []string, concurrency int, handler Handler[T]) *Worker[T] {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     KafkaConfig.Brokers,
		GroupID:     group,
		GroupTopics: topics,
		MinBytes:    1e3,
		MaxBytes:    10e6,
	})
	return &Worker[T]{r: r, sem: make(chan struct{}, concurrency), unmarshal: json.Unmarshal, handle: handler}
}

func (w *Worker[T]) Run(ctx context.Context) error {
	for {
		m, err := w.r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		w.sem <- struct{}{}
		go func(m kafka.Message) {
			defer func() { <-w.sem }()
			var val T
			_ = w.unmarshal(m.Value, &val)
			h := map[string]string{}
			for _, x := range m.Headers {
				h[string(x.Key)] = string(x.Value)
			}
			err := w.handle(ctx, Message[T]{
				Topic:     m.Topic,
				Value:     val,
				Key:       string(m.Key),
				Headers:   h,
				Raw:       m,
				SessionID: h["sessionId"],
			})
			if err != nil {
				log.Error("handle kafka failed: ", err.Error())
			} else {
				_ = w.r.CommitMessages(ctx, m)
			}
		}(m)
	}
}

func (w *Worker[T]) Close() error { return w.r.Close() }
