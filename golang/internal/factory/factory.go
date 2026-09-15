package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	_, err = base.ch.QueueDeclare(
		queueName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		base.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	return &QueueMiddleware{
		BaseMiddleware: base,
		queueName:      queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
