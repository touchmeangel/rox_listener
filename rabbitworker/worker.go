package rabbitworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type EventHandler func(ctx context.Context, data json.RawMessage) error

type incomingMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type Worker struct {
	amqpURL   string
	queueName string

	mu       sync.RWMutex
	handlers map[string]EventHandler

	conn    *amqp.Connection
	channel *amqp.Channel

	wg          sync.WaitGroup
	reconnectAt time.Duration
	logger      *slog.Logger
}

type Option func(*Worker)

func WithLogger(l *slog.Logger) Option {
	return func(w *Worker) { w.logger = l }
}

func WithReconnectDelay(d time.Duration) Option {
	return func(w *Worker) { w.reconnectAt = d }
}

func New(amqpURL, queueName string, opts ...Option) *Worker {
	w := &Worker{
		amqpURL:     amqpURL,
		queueName:   queueName,
		handlers:    make(map[string]EventHandler),
		reconnectAt: 3 * time.Second,
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *Worker) On(event string, handler EventHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[event] = handler
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		err := w.connectAndConsume(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}

		w.logger.Error("connection lost, reconnecting", "error", err, "delay", w.reconnectAt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.reconnectAt):
		}
	}
}

func (w *Worker) connectAndConsume(ctx context.Context) error {
	conn, err := amqp.Dial(w.amqpURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(w.queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue %q: %w", w.queueName, err)
	}

	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	w.mu.Lock()
	w.conn, w.channel = conn, ch
	w.mu.Unlock()

	closeNotify := conn.NotifyClose(make(chan *amqp.Error, 1))
	w.logger.Info("ready to accept tasks", "queue", q.Name)

	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr, ok := <-closeNotify:
			if !ok || amqpErr == nil {
				return errors.New("connection closed")
			}
			return amqpErr
		case msg, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed unexpectedly")
			}
			w.dispatch(ctx, msg)
		}
	}
}

func (w *Worker) dispatch(ctx context.Context, msg amqp.Delivery) {
	var incoming incomingMessage
	if err := json.Unmarshal(msg.Body, &incoming); err != nil {
		w.logger.Warn("malformed message body, dropping", "error", err)
		_ = msg.Nack(false, false) // don't requeue — it'll never parse
		return
	}

	w.logger.Info("received event", "event", incoming.Event)

	w.mu.RLock()
	handler, ok := w.handlers[incoming.Event]
	w.mu.RUnlock()

	if !ok {
		w.logger.Warn("unhandled event", "event", incoming.Event)
		_ = msg.Ack(false)
		return
	}

	if err := msg.Ack(false); err != nil {
		w.logger.Error("ack failed", "event", incoming.Event, "error", err)
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				w.logger.Error("handler panicked", "event", incoming.Event, "panic", r)
			}
		}()
		if err := handler(ctx, incoming.Data); err != nil {
			w.logger.Error("handler error", "event", incoming.Event, "error", err)
		}
	}()
}

func (w *Worker) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		w.logger.Warn("shutdown timed out with handlers still in flight")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.channel != nil {
		_ = w.channel.Close()
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
