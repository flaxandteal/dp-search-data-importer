package event

import (
	"context"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/v2/log"
)

// MessageConsumer provides a generic interface for consuming []byte messages (from Kafka)
type MessageConsumer interface {
	Channels() *kafka.ConsumerGroupChannels
}

// Consumer consumes event messages.
type Consumer struct {
	closing chan eventClose
	closed  chan bool
}

// NewConsumer returns a new consumer instance.
func NewConsumer() *Consumer {
	return &Consumer{
		closing: make(chan eventClose),
		closed:  make(chan bool),
	}
}

type eventClose struct {
	ctx context.Context
}

// Handler represents a handler for processing a single event.
type Handler interface {
	Handle(ctx context.Context,
		SearchDataImportModel []*models.SearchDataImportModel) error
}

// Consume converts messages to event instances, and pass the event to the provided handler.
func (consumer *Consumer) Consume(
	ctx context.Context,
	messageConsumer MessageConsumer,
	handler Handler,
	cfg *config.Config) {

	go func() {
		defer close(consumer.closed)

		batch := NewBatch(cfg.BatchSize)
		// Wait a batch full of messages.
		// If we do not get any messages for a time, just process the messages already in the batch.
		for {
			select {
			case msg := <-messageConsumer.Channels().Upstream:
				AddMessageToBatch(ctx, batch, msg, handler)
				msg.Release()

			case <-time.After(cfg.BatchWaitTime):
				if batch.IsEmpty() {
					continue
				}
				ProcessBatch(ctx, handler, batch, "timeout")

			case <-consumer.closing:
				log.Info(ctx, "closing event consumer loop")
				close(consumer.closing)
				return
			}
		}
	}()
}

// AddMessageToBatch will attempt to add the message to the batch and determine if it should be processed.
func AddMessageToBatch(ctx context.Context, batch *Batch, msg kafka.Message, handler Handler) {
	log.Info(ctx, "add message to batch starts")
	batch.Add(ctx, msg)
	if batch.IsFull() {
		ProcessBatch(ctx, handler, batch, "full-batch")
	}
}

// ProcessBatch will attempt to handle and commit the batch, or shutdown if something goes horribly wrong.
func ProcessBatch(ctx context.Context, handler Handler, batch *Batch, reason string) {
	log.Info(ctx, "process batch starts", log.Data{
		"batch_size":                batch.Size(),
		"reason":                    reason})
	err := handler.Handle(ctx, batch.Events())
	if err != nil {
		log.Error(ctx, "error handling batch", err)
		return
	}

	//Handle Batch Events..committing " Ends"
	log.Info(ctx, "batch event processed - committing")
	batch.Commit()
}

// Close safely closes the consumer and releases all resources
func (consumer *Consumer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	consumer.closing <- eventClose{ctx: ctx}

	select {
	case <-consumer.closed:
		log.Info(ctx, "successfully closed event consumer")
		return nil
	case <-ctx.Done():
		log.Error(ctx, "shutdown context time exceeded, skipping graceful shutdown of event consumer", ctx.Err())
		return ctx.Err()
	}
}
