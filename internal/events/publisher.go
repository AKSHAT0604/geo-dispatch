package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Publisher publishes lifecycle events. It's an interface so packages that
// emit events (the offer dispatcher, driver location updates) don't need a
// live Kafka broker to be unit tested.
type Publisher interface {
	PublishDriverLocation(ctx context.Context, e DriverLocationEvent) error
	PublishTripLifecycle(ctx context.Context, e TripLifecycleEvent) error
	PublishOffer(ctx context.Context, e OfferEvent) error
}

// NoopPublisher discards every event. It's the default when no broker is
// configured, so the rest of the system runs identically with or without
// Kafka available.
type NoopPublisher struct{}

func (NoopPublisher) PublishDriverLocation(context.Context, DriverLocationEvent) error { return nil }
func (NoopPublisher) PublishTripLifecycle(context.Context, TripLifecycleEvent) error   { return nil }
func (NoopPublisher) PublishOffer(context.Context, OfferEvent) error                   { return nil }

// KafkaPublisher publishes events as JSON to the three topics, one writer
// per topic so each can be tuned (batching, acks) independently.
type KafkaPublisher struct {
	driverLocation *kafka.Writer
	tripLifecycle  *kafka.Writer
	offers         *kafka.Writer
}

// NewKafkaPublisher returns a KafkaPublisher connected to brokers. Each
// writer hashes on the message key so a given driver's or trip's events
// always land on the same partition, preserving per-entity order.
func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
		}
	}
	return &KafkaPublisher{
		driverLocation: newWriter(TopicDriverLocation),
		tripLifecycle:  newWriter(TopicTripLifecycle),
		offers:         newWriter(TopicOfferEvents),
	}
}

func (p *KafkaPublisher) PublishDriverLocation(ctx context.Context, e DriverLocationEvent) error {
	return publish(ctx, p.driverLocation, e.DriverID, e)
}

func (p *KafkaPublisher) PublishTripLifecycle(ctx context.Context, e TripLifecycleEvent) error {
	return publish(ctx, p.tripLifecycle, e.TripID, e)
}

func (p *KafkaPublisher) PublishOffer(ctx context.Context, e OfferEvent) error {
	return publish(ctx, p.offers, e.TripID, e)
}

// Close flushes and closes every underlying writer.
func (p *KafkaPublisher) Close() error {
	return errors.Join(p.driverLocation.Close(), p.tripLifecycle.Close(), p.offers.Close())
}

func publish(ctx context.Context, w *kafka.Writer, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: body}); err != nil {
		return fmt.Errorf("write to %s: %w", w.Topic, err)
	}
	return nil
}
