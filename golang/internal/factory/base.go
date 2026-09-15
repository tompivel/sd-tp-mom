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

func (b *BaseMiddleware) StartConsumingQueue(queueName string, callbackFunc func(msg m.Message, ack func(), nack func())) error {
	b.mu.Lock()
	if b.isConsuming {
		b.mu.Unlock()
		return fmt.Errorf("already consuming")
	}
	
	if b.conn.IsClosed() {
		b.mu.Unlock()
		return m.ErrMessageMiddlewareDisconnected
	}

	// unique consumer tag
	b.consumerTag = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	b.isConsuming = true
	b.mu.Unlock()

	msgs, err := b.ch.Consume(
		queueName,
		b.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	
	if err != nil {
		b.mu.Lock()
		b.isConsuming = false
		b.mu.Unlock()
		
		if b.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			msg := m.Message{Body: string(d.Body)}
			
			delivery := d
			ack := func() { delivery.Ack(false) }
			nack := func() { delivery.Nack(false, true) }
			
			callbackFunc(msg, ack, nack)
		}
	}()

	return nil
}

func (b *BaseMiddleware) StopConsuming() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	if !b.isConsuming {
		return nil
	}

	err := b.ch.Cancel(b.consumerTag, false)
	if err != nil {
		if b.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareClose
	}

	b.isConsuming = false
	return nil
}

