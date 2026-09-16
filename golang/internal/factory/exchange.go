package factory

import (
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
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

	keys := e.routingKeys
	if len(keys) == 0 {
		keys = []string{""}
	}

	for _, routingKey := range keys {
		err := e.BaseMiddleware.PublishWithTimeout(e.exchangeName, routingKey, msg, 5*time.Second)
		if err != nil {
			return err
		}
	}

	return nil
}
