package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	*BaseMiddleware
	queueName string
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return q.BaseMiddleware.StartConsumingQueue(q.queueName, callbackFunc)
}

