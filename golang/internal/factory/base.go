package factory

import (
	"fmt"
	"sync"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	mu           sync.Mutex
	isConsuming  bool
	consumerTag  string
}

func NewBaseMiddleware(settings m.ConnSettings) (*BaseMiddleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	return &BaseMiddleware{
		conn: conn,
		ch:   ch,
	}, nil
}

