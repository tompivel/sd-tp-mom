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

