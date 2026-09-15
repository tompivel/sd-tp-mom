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
		Transient, // durable
		Keep,      // delete when unused
		Shared,    // exclusive
		Wait,      // no-wait
		nil,       // arguments
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
		TopicExchange,  // type
		Transient,      // durable
		Keep,           // auto-deleted
		NonInternal,    // internal
		Wait,           // no-wait
		nil,            // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	q, err := base.ch.QueueDeclare(
		"",          // auto-generated
		Transient,   // durable
		AutoDelete,  // auto-delete
		Exclusive,   // exclusive
		Wait,        // no-wait
		nil,         // arguments
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range keys {
		err = base.ch.QueueBind(
			q.Name,
			key,
			exchange,
			Wait, // no-wait
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
