package redis

import (
	"context"
	"testing"
	"time"
)

func TestPubSubProvider_Publish(t *testing.T) {
	client := setupRedis(t)
	channel := "herald-test-pubsub-publish"

	provider := NewPubSub(channel, WithPubSubClient(client))

	err := provider.Publish(context.Background(), []byte(`{"test":"data"}`), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPubSubProvider_PublishNoClient(t *testing.T) {
	provider := NewPubSub("test-channel")

	err := provider.Publish(context.Background(), []byte(`{"test":"data"}`), nil)
	if err == nil {
		t.Fatal("expected error with nil client")
	}
}

func TestPubSubProvider_Subscribe(t *testing.T) {
	client := setupRedis(t)
	channel := "herald-test-pubsub-subscribe"

	provider := NewPubSub(channel, WithPubSubClient(client))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := provider.Subscribe(ctx)

	// Give the subscription time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish messages after subscribing
	client.Publish(context.Background(), channel, `{"order":"1"}`)
	client.Publish(context.Background(), channel, `{"order":"2"}`)

	var received [][]byte
	for i := 0; i < 2; i++ {
		select {
		case result := <-ch:
			if result.IsError() {
				t.Fatalf("unexpected error: %v", result.Error())
			}
			received = append(received, result.Value().Data)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}

	cancel()

	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}

	if string(received[0]) != `{"order":"1"}` {
		t.Errorf("unexpected first message: %s", received[0])
	}
	if string(received[1]) != `{"order":"2"}` {
		t.Errorf("unexpected second message: %s", received[1])
	}
}

func TestPubSubProvider_SubscribeNoClient(t *testing.T) {
	provider := NewPubSub("test-channel")

	ctx := context.Background()
	ch := provider.Subscribe(ctx)

	result, ok := <-ch
	if !ok {
		t.Fatal("expected error result before close")
	}
	if !result.IsError() {
		t.Error("expected error result")
	}

	_, ok = <-ch
	if ok {
		t.Error("expected closed channel after error")
	}
}

func TestPubSubProvider_AckNack(t *testing.T) {
	client := setupRedis(t)
	channel := "herald-test-pubsub-ack"

	provider := NewPubSub(channel, WithPubSubClient(client))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := provider.Subscribe(ctx)

	time.Sleep(100 * time.Millisecond)

	client.Publish(context.Background(), channel, `{"test":"ack"}`)

	select {
	case result := <-ch:
		if result.IsError() {
			t.Fatalf("unexpected error: %v", result.Error())
		}
		msg := result.Value()
		if err := msg.Ack(); err != nil {
			t.Errorf("ack should be no-op: %v", err)
		}
		if err := msg.Nack(); err != nil {
			t.Errorf("nack should be no-op: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPubSubProvider_Ping(t *testing.T) {
	client := setupRedis(t)
	provider := NewPubSub("test-channel", WithPubSubClient(client))

	err := provider.Ping(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPubSubProvider_PingNoClient(t *testing.T) {
	provider := NewPubSub("test-channel")

	err := provider.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error with nil client")
	}
}

func TestPubSubProvider_Close(t *testing.T) {
	client := setupRedis(t)
	provider := NewPubSub("test-channel", WithPubSubClient(client))

	err := provider.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPubSubProvider_CloseNil(t *testing.T) {
	provider := NewPubSub("test-channel")

	err := provider.Close()
	if err != nil {
		t.Fatalf("unexpected error with nil client: %v", err)
	}
}
