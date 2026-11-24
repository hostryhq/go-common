package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hostryhq/go-common/inbox"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Connect opens a connection for service with its nkey seed (read from seedFile, a file
// secret; never logged). The reply prefix is restricted to the service so that its
// permissions can be narrow.
func Connect(url, service, seedFile string, opts ...nats.Option) (*nats.Conn, error) {
	return connect(url, InboxPrefix(service), seedFile, opts...)
}

// ConnectAdmin opens the provisioning connection (full rights; services never hold it).
func ConnectAdmin(url, seedFile string, opts ...nats.Option) (*nats.Conn, error) {
	return connect(url, "_INBOX."+AdminUser, seedFile, opts...)
}

func connect(url, inbox, seedFile string, opts ...nats.Option) (*nats.Conn, error) {
	seed, err := os.ReadFile(seedFile)
	if err != nil {
		return nil, fmt.Errorf("bus: read seed: %w", err)
	}
	kp, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
	if err != nil {
		return nil, fmt.Errorf("bus: bad seed: %w", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	all := append([]nats.Option{
		nats.Nkey(pub, func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) }),
		nats.CustomInboxPrefix(inbox),
		nats.Timeout(5 * time.Second),
	}, opts...)
	return nats.Connect(url, all...)
}

// ProvisionOptions sets stream and consumer limits (ADR-0011 B5, B6, B11).
type ProvisionOptions struct {
	// MaxAge of a stream buffer; default 7 days.
	MaxAge time.Duration
	// MaxBytes per stream; default 1 GiB.
	MaxBytes int64
	// AckWait, BackOff and MaxDeliver of every consumer; defaults 30s, 1s..10m, 10.
	AckWait    time.Duration
	BackOff    []time.Duration
	MaxDeliver int
	// Generation is recorded in stream metadata (bus generation id, B9).
	Generation string
}

// DefaultBackOff is the provisional redelivery schedule of ADR-0011 B5 (10 attempts).
var DefaultBackOff = []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
	time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 10 * time.Minute, 10 * time.Minute}

func (o *ProvisionOptions) defaults() {
	if o.MaxAge == 0 {
		o.MaxAge = 7 * 24 * time.Hour
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = 1 << 30
	}
	if o.AckWait == 0 {
		o.AckWait = 30 * time.Second
	}
	if len(o.BackOff) == 0 {
		o.BackOff = DefaultBackOff
	}
	if o.MaxDeliver == 0 {
		o.MaxDeliver = len(o.BackOff)
	}
}

// Provision creates or updates, idempotently, one stream EVT_<SERVICE> per publisher and one
// durable pull consumer per consuming service per stream, from the registry. It must run
// with the admin credential. Streams refuse new messages when full (discard new).
func Provision(ctx context.Context, nc *nats.Conn, reg *Registry, o ProvisionOptions) error {
	o.defaults()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	for _, svc := range reg.Publishers() {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       StreamName(svc),
			Subjects:   []string{"evt." + svc + ".>"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     o.MaxAge,
			MaxBytes:   o.MaxBytes,
			Discard:    jetstream.DiscardNew,
			Duplicates: 2 * time.Minute,
			Metadata:   map[string]string{"hostry_generation": o.Generation},
		})
		if err != nil {
			return fmt.Errorf("bus: stream %s: %w", StreamName(svc), err)
		}
	}
	for _, c := range reg.Consumptions() {
		cfg := jetstream.ConsumerConfig{
			Durable:       c.Consumer,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       o.AckWait,
			BackOff:       o.BackOff,
			MaxDeliver:    o.MaxDeliver,
			MaxAckPending: 1, // strict order per consumer; raise by partitioning (B12)
			DeliverPolicy: jetstream.DeliverAllPolicy,
		}
		if len(c.Subjects) == 1 {
			cfg.FilterSubject = c.Subjects[0]
		} else {
			cfg.FilterSubjects = c.Subjects
		}
		if _, err := js.CreateOrUpdateConsumer(ctx, c.Stream, cfg); err != nil {
			return fmt.Errorf("bus: consumer %s on %s: %w", c.Consumer, c.Stream, err)
		}
	}
	return nil
}

// ErrStreamFull is returned by Publish when the stream refuses new messages (discard new).
// The relay must keep the outbox row and back off.
var ErrStreamFull = errors.New("bus: stream is full, publish refused")

// Publisher publishes to JetStream and waits for the stream acknowledgement. It satisfies outbox.Sink.
type Publisher struct{ js jetstream.JetStream }

// NewPublisher wraps nc.
func NewPublisher(nc *nats.Conn) (*Publisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &Publisher{js: js}, nil
}

// Publish sends data on subject with Nats-Msg-Id = id (de-duplication) and waits for the ack.
// A duplicate within the window is acknowledged as success.
func (p *Publisher) Publish(ctx context.Context, subject, id string, data []byte) error {
	_, err := p.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data}, jetstream.WithMsgID(id))
	if err == nil {
		return nil
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && isFullErr(apiErr) {
		return fmt.Errorf("%w: %w", ErrStreamFull, err)
	}
	return err
}

// Delivery is one message handed to a subscriber. It satisfies inbox.Delivery.
type Delivery struct{ msg jetstream.Msg }

// Subject returns the message subject.
func (d Delivery) Subject() string { return d.msg.Subject() }

// Data returns the raw envelope JSON.
func (d Delivery) Data() []byte { return d.msg.Data() }

// NumDelivered is the 1-based delivery attempt of this message.
func (d Delivery) NumDelivered() uint64 {
	md, err := d.msg.Metadata()
	if err != nil {
		return 1
	}
	return md.NumDelivered
}

// Ack acknowledges and waits for the server to confirm.
func (d Delivery) Ack() error { return d.msg.DoubleAck(context.Background()) }

// Nak asks for redelivery after delay (0 means the consumer's back-off).
func (d Delivery) Nak(delay time.Duration) error {
	if delay <= 0 {
		return d.msg.Nak()
	}
	return d.msg.NakWithDelay(delay)
}

// Consumer pulls from an existing durable consumer. Services cannot create consumers.
type Consumer struct{ c jetstream.Consumer }

// ErrNoMessage means nothing arrived within the wait.
var ErrNoMessage = errors.New("bus: no message")

// BindConsumer binds to the durable consumer of service on stream (published by publisher).
func BindConsumer(ctx context.Context, nc *nats.Conn, publisher, service string) (*Consumer, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	c, err := js.Consumer(ctx, StreamName(publisher), service)
	if err != nil {
		return nil, err
	}
	return &Consumer{c: c}, nil
}

// Next waits up to wait for one message. It returns ErrNoMessage on timeout.
func (c *Consumer) Next(ctx context.Context, wait time.Duration) (Delivery, error) {
	batch, err := c.c.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		return Delivery{}, err
	}
	for m := range batch.Messages() {
		return Delivery{msg: m}, nil
	}
	if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
		return Delivery{}, err
	}
	if err := ctx.Err(); err != nil {
		return Delivery{}, err
	}
	return Delivery{}, ErrNoMessage
}

// SignalListener subscribes (plain, at-most-once, no consumer state) to a signal subject
// (ADR-0011 B3). A missed signal is harmless only if the caller states a time bound without it.
func SignalListener(nc *nats.Conn, subject string, fn func(subject string, data []byte)) (unsubscribe func() error, err error) {
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) { fn(m.Subject, m.Data) })
	if err != nil {
		return nil, err
	}
	return sub.Unsubscribe, nil
}

// isFullErr is refined by the E6/E-full integration test against the real server.
func isFullErr(e *jetstream.APIError) bool {
	return e.ErrorCode == 10077 || e.ErrorCode == 10054
}

// Source adapts the consumer to inbox.Source with a fixed per-poll wait.
func (c *Consumer) Source(wait time.Duration) inbox.Source { return source{c: c, wait: wait} }

type source struct {
	c    *Consumer
	wait time.Duration
}

func (s source) Next(ctx context.Context) (inbox.Delivery, error) {
	d, err := s.c.Next(ctx, s.wait)
	if errors.Is(err, ErrNoMessage) {
		return nil, inbox.ErrNoMessage
	}
	if err != nil {
		return nil, err
	}
	return d, nil
}
