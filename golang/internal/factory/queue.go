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

func (q *QueueMiddleware) Send(msg m.Message) error {
	if q.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	//TODO: Define constant for default exchange
	//TODO: Create context with 5s timeout
	//TODO: Persistent messages?
	err := q.ch.PublishWithContext(
		context.Background(),
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if q.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}
