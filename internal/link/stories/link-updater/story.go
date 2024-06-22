package link_updater

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/scrape"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func New(repository repository, consumer amqpConsumer) *Story {
	return &Story{repository: repository, consumer: consumer}
}

type Story struct {
	repository repository
	consumer   amqpConsumer
}

func (s *Story) Run(ctx context.Context) error {
	// implemented
	msgCh, err := s.consumer.Consume(
		"link_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("channel Consume: %w", err)
	}

	go func() {
		for in := range msgCh {
			// Слушаем очередь и вызываем пакет scrape
			// Сообщение которое получаем с rabbitmq

			// Получаем текущий объект ссылки
			// Добавляем данные из scrape
			// Обновляем данные в DB

			type message struct {
				ID string `json:"id"`
			}

			var m message
			if err := json.Unmarshal(in.Body, &m); err != nil {
				_ = fmt.Errorf("json Unmarshal: %w", err)
				continue
			}

			parsedId, err := primitive.ObjectIDFromHex(m.ID)
			if err != nil {
				_ = fmt.Errorf("primitive ObjectIDFromHex: %w", err)
				continue
			}

			l, err := s.repository.FindByID(ctx, parsedId)
			if err != nil {
				_ = fmt.Errorf("links repository FindByID: %w", err)
				continue
			}

			parsedHtml, err := scrape.Parse(ctx, l.URL)
			if err != nil {
				_ = fmt.Errorf("scrape Parse: %w", err)
				continue
			}

			req := database.UpdateLinkReq{
				ID:     parsedId,
				URL:    l.URL,
				Title:  l.Title,
				Images: l.Images,
				UserID: l.UserID,
				Tags:   l.Tags,
			}

			if len(parsedHtml.Title) > 0 {
				req.Title = parsedHtml.Title
			}

			if len(parsedHtml.Tags) > 0 {
				req.Tags = parsedHtml.Tags
			}

			if _, err := s.repository.Update(ctx, req); err != nil {
				_ = fmt.Errorf("links repository Update: %w", err)
				continue
			}
		}
	}()

	<-ctx.Done()

	return nil
}
