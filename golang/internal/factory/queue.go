package factory

import (
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
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
	//TODO: Persistent messages?
	return q.BaseMiddleware.PublishWithTimeout("", q.queueName, msg, 5*time.Second)
}
