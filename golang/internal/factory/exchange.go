package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	*BaseMiddleware
	exchangeName string
	routingKeys  []string
	queueName    string
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return e.BaseMiddleware.StartConsumingQueue(e.queueName, callbackFunc)
}

