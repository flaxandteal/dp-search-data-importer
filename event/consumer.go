package event

import (
	"context"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
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
	closing chan bool
	Closed  chan bool
}

// NewConsumer returns a new consumer instance.
func NewConsumer() *Consumer {
	return &Consumer{
		closing: make(chan bool),
		Closed:  make(chan bool),
	}
}

// Handler represents a handler for processing a single event.
type Handler interface {
	Handle(ctx context.Context,
		esDestURL string,
		SearchDataImportModel []*models.SearchDataImportModel) error
}

// Consume converts messages to event instances, and pass the event to the provided handler.
func (consumer *Consumer) Consume(
	ctx context.Context,
	messageConsumer MessageConsumer,
	batchHandler Handler,
	cfg *config.Config) {

	go func() {
		defer close(consumer.Closed)

		batch := NewBatch(cfg.BatchSize)
		// Wait a batch full of messages.
		// If we do not get any messages for a time, just process the messages already in the batch.
		for {
			delay := time.NewTimer(cfg.BatchWaitTime)
			select {
			case msg := <-messageConsumer.Channels().Upstream:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}

				AddMessageToBatch(ctx, cfg, batch, msg, batchHandler)
				msg.Release()

			case <-delay.C:
				if batch.IsEmpty() {
					continue
				}
				ProcessBatch(ctx, cfg, batchHandler, batch, "timeout")

			case <-consumer.closing:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				log.Info(ctx, "closing event consumer loop")
				close(consumer.closing)
				return
			}
		}
	}()
}

// AddMessageToBatch will attempt to add the message to the batch and determine if it should be processed.
func AddMessageToBatch(ctx context.Context, cfg *config.Config, batch *Batch, msg kafka.Message, handler Handler) {
	log.Info(ctx, "add message to batch starts")

	event, err := Unmarshal(msg)
	if err != nil {
		log.Error(ctx, "failed to unmarshal event", err)
		return
	}

	ctx = dprequest.WithRequestId(ctx, event.TraceID)
	log.Info(ctx, "event received to be added into the batch")

	batch.messages = append(batch.messages, msg)
	batch.Add(ctx, event)
	if batch.IsFull() {
		ProcessBatch(ctx, cfg, handler, batch, "full-batch")
	}
}

// ProcessBatch will attempt to handle and commit the batch, or shutdown if something goes horribly wrong.
func ProcessBatch(ctx context.Context, cfg *config.Config, handler Handler, batch *Batch, reason string) {
	log.Info(ctx, "process batch starts", log.Data{
		"batch_size": batch.Size(),
		"reason":     reason})
	err := handler.Handle(ctx, cfg.ElasticSearchAPIURL, batch.Events())
	if err != nil {
		log.Error(ctx, "error handling batch", err)
		batch.Commit()
		return
	}

	log.Info(ctx, "batch event processed - committing")
	batch.Commit()
}

// Close safely closes the consumer and releases all resources
func (consumer *Consumer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	consumer.closing <- true

	select {
	case <-consumer.Closed:
		log.Info(ctx, "successfully closed event consumer")
		return nil
	case <-ctx.Done():
		log.Error(ctx, "shutdown context time exceeded, skipping graceful shutdown of event consumer", ctx.Err())
		return ctx.Err()
	}
}
