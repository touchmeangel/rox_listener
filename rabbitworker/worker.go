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

type EventHandler func(ctx context.Context, data json.RawMessage) (json.RawMessage, error)

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

type incomingMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type Worker struct {
	amqpURL   string
	queueName string
	prefetch  int

	mu        sync.RWMutex
	handlers  map[string]EventHandler
	conn      *amqp.Connection
	consumeCh *amqp.Channel
	publishCh *amqp.Channel

	wg          sync.WaitGroup
	reconnectAt time.Duration
	logger      *slog.Logger
}

type Option func(*Worker)

func WithLogger(l *slog.Logger) Option          { return func(w *Worker) { w.logger = l } }
func WithReconnectDelay(d time.Duration) Option { return func(w *Worker) { w.reconnectAt = d } }
func WithPrefetch(n int) Option                 { return func(w *Worker) { w.prefetch = n } }

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
	defer func() { _ = conn.Close() }()

	consumeCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open consume channel: %w", err)
	}
	defer func() { _ = consumeCh.Close() }()

	if w.prefetch > 0 {
		if err := consumeCh.Qos(w.prefetch, 0, false); err != nil {
			return fmt.Errorf("set qos: %w", err)
		}
	}

	q, err := consumeCh.QueueDeclare(w.queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue %q: %w", w.queueName, err)
	}

	deliveries, err := consumeCh.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open publish channel: %w", err)
	}
	defer func() { _ = publishCh.Close() }()

	w.mu.Lock()
	w.conn, w.consumeCh, w.publishCh = conn, consumeCh, publishCh
	w.mu.Unlock()

	closeNotify := conn.NotifyClose(make(chan *amqp.Error, 1))
	w.logger.Info("ready to accept tasks", "queue", q.Name, "prefetch", w.prefetch)

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
		_ = msg.Nack(false, false)
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

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		var (
			response json.RawMessage
			err      error
		)
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("handler panicked: %v", r)
				}
			}()
			response, err = handler(ctx, incoming.Data)
		}()

		if err != nil {
			w.logger.Error("handler error", "event", incoming.Event, "error", err)

			var permErr *PermanentError
			if errors.As(err, &permErr) {
				if ackErr := msg.Ack(false); ackErr != nil {
					w.logger.Error("ack failed", "event", incoming.Event, "error", ackErr)
				}
				if msg.ReplyTo != "" {
					w.publishReply(ctx, msg, errorReplyBody(err))
				}
				return
			}

			if nackErr := msg.Nack(false, true); nackErr != nil {
				w.logger.Error("nack failed", "event", incoming.Event, "error", nackErr)
			}
			return
		}

		if ackErr := msg.Ack(false); ackErr != nil {
			w.logger.Error("ack failed", "event", incoming.Event, "error", ackErr)
			return
		}
		if msg.ReplyTo != "" {
			w.publishReply(ctx, msg, response)
		}
	}()
}

func (w *Worker) getPublishChannel() (*amqp.Channel, error) {
	w.mu.RLock()
	ch, conn := w.publishCh, w.conn
	w.mu.RUnlock()

	if ch != nil && !ch.IsClosed() {
		return ch, nil
	}
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("no active connection")
	}

	newCh, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("reopening publish channel: %w", err)
	}

	w.mu.Lock()
	w.publishCh = newCh
	w.mu.Unlock()
	return newCh, nil
}

func (w *Worker) publishReply(ctx context.Context, msg amqp.Delivery, body json.RawMessage) {
	ch, err := w.getPublishChannel()
	if err != nil {
		w.logger.Error("cannot publish reply", "reply_to", msg.ReplyTo, "error", err)
		return
	}

	err = ch.PublishWithContext(ctx, "", msg.ReplyTo, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: msg.CorrelationId,
		Body:          body,
	})
	if err != nil {
		w.logger.Error("publishing reply failed", "reply_to", msg.ReplyTo, "error", err)
	}
}

func errorReplyBody(err error) json.RawMessage {
	body, marshalErr := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: err.Error()})
	if marshalErr != nil {
		return json.RawMessage(`{"error":"unknown error"}`)
	}
	return body
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
	if w.publishCh != nil {
		_ = w.publishCh.Close()
	}
	if w.consumeCh != nil {
		_ = w.consumeCh.Close()
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
