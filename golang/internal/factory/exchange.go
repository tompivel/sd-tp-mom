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
	// The queue is already created and bound during initialization.
	return e.BaseMiddleware.StartConsumingQueue(e.queueName, callbackFunc)
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	if e.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	// TODO: If there is more than one routing key provided, send the same message to each routine key.
	// TODO: PublishWithContext could be encapsulated inside a method for BaseMiddleware, maybe "PublishWithTimeout".

	routingKey := ""
	if len(e.routingKeys) > 0 {
		routingKey = e.routingKeys[0]
	}

	err := e.ch.PublishWithContext(
		context.Background(),
		e.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if e.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}
