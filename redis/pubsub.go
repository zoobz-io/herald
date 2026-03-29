package redis

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/zoobz-io/herald"
)

// PubSubProvider implements herald.Provider for Redis Pub/Sub.
// Unlike the Streams provider, Pub/Sub is ephemeral — messages are
// lost if no subscriber is listening. Every subscriber receives every
// message (broadcast semantics).
type PubSubProvider struct {
	client  *redis.Client
	sub     *redis.PubSub
	channel string
}

// PubSubOption configures a PubSubProvider.
type PubSubOption func(*PubSubProvider)

// WithPubSubClient sets the Redis client.
func WithPubSubClient(c *redis.Client) PubSubOption {
	return func(p *PubSubProvider) {
		p.client = c
	}
}

// NewPubSub creates a Redis Pub/Sub provider for the given channel.
func NewPubSub(channel string, opts ...PubSubOption) *PubSubProvider {
	p := &PubSubProvider{
		channel: channel,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish sends raw bytes to a Redis Pub/Sub channel.
// Note: Redis Pub/Sub does not support metadata; metadata is ignored.
func (p *PubSubProvider) Publish(ctx context.Context, data []byte, metadata herald.Metadata) error {
	if p.client == nil {
		return herald.ErrNoWriter
	}
	return p.client.Publish(ctx, p.channel, data).Err()
}

// Subscribe returns a stream of messages from a Redis Pub/Sub channel.
func (p *PubSubProvider) Subscribe(ctx context.Context) <-chan herald.Result[herald.Message] {
	out := make(chan herald.Result[herald.Message])

	if p.client == nil {
		go func() {
			out <- herald.NewError[herald.Message](herald.ErrNoReader)
			close(out)
		}()
		return out
	}

	p.sub = p.client.Subscribe(ctx, p.channel)

	go func() {
		defer close(out)

		ch := p.sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				heraldMsg := herald.Message{
					Data: []byte(msg.Payload),
					Ack: func() error {
						return nil
					},
					Nack: func() error {
						return nil
					},
				}
				select {
				case out <- herald.NewSuccess(heraldMsg):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

// Ping verifies Redis connectivity.
func (p *PubSubProvider) Ping(ctx context.Context) error {
	if p.client == nil {
		return herald.ErrNoWriter
	}
	return p.client.Ping(ctx).Err()
}

// Close releases Redis Pub/Sub resources.
func (p *PubSubProvider) Close() error {
	var subErr error
	if p.sub != nil {
		subErr = p.sub.Close()
	}
	if p.client != nil {
		return errors.Join(subErr, p.client.Close())
	}
	return subErr
}
