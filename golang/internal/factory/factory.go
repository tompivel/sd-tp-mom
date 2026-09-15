package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	_, err = base.ch.QueueDeclare(
		queueName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	success = true
	return &QueueMiddleware{
		BaseMiddleware: base,
		queueName:      queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	err = base.ch.ExchangeDeclare(
		exchange,
		"topic", // type
		false,   // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	q, err := base.ch.QueueDeclare(
		"",    // auto-generated
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range keys {
		err = base.ch.QueueBind(
			q.Name,
			key,
			exchange,
			false,
			nil,
		)
		if err != nil {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
	}

	success = true
	return &ExchangeMiddleware{
		BaseMiddleware: base,
		exchangeName:   exchange,
		routingKeys:    keys,
		queueName:      q.Name,
	}, nil
}
