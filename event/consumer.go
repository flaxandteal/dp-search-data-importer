package event

import (
	"context"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
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
		PublishedContentModel []*models.PublishedContentModel) error
}

// Consume converts messages to event instances, and pass the event to the provided handler.
func (consumer *Consumer) Consume(
	ctx context.Context,
	messageConsumer MessageConsumer,
	handler Handler,
	cfg *config.Config) {

	// consume loop, to be executed by each worker
	var consume = func(workerID int) {
		for {
			select {
			case message, ok := <-messageConsumer.Channels().Upstream:
				if !ok {
					log.Info(ctx, "closing event consumer loop because upstream channel is closed", log.Data{"worker_id": workerID})
					return
				}
				messageCtx := context.Background()
				processMessage(messageCtx, messageConsumer, message, handler, cfg, consumer)
				message.Release()
			case <-messageConsumer.Channels().Closer:
				log.Info(ctx, "closing event consumer loop because closer channel is closed", log.Data{"worker_id": workerID})
				return
			}
		}
	}

	// workers to consume messages in parallel
	for w := 1; w <= cfg.KafkaNumWorkers; w++ {
		go consume(w)
	}
}

// processMessage unmarshals the provided kafka message into an event and calls the handler.
// After the message is handled, it is committed.
func processMessage(ctx context.Context,
	messageConsumer MessageConsumer,
	message kafka.Message,
	handler Handler,
	cfg *config.Config,
	consumer *Consumer) {

	// unmarshal - commit on failure (consuming the message again would result in the same error)
	event, err := unmarshal(message)
	if err != nil {
		log.Error(ctx, "failed to unmarshal event", err)
		message.Commit()
		return
	}

	log.Info(ctx, "event received", log.Data{"event": event})

	//Handle Batch Events : Starts
	start := time.Now()
	log.Info(ctx, "batch processing starts", log.Data{"batch start time": start})

	batchSize := cfg.BatchSize
	batchWaitTime := cfg.BatchWaitTime
	batch := NewBatch(batchSize)

	defer close(consumer.closed)

	// Wait a batch full of messages.
	// If we do not get any messages for a time, just process the messages already in the batch.
	for {
		select {
		case msg := <-messageConsumer.Channels().Upstream:
			ctx := context.Background()

			batch.Add(ctx, msg)
			if batch.IsFull() {
				log.Event(ctx, "batch is full - processing batch", log.INFO, log.Data{"batchsize": batch.Size()})
				ProcessBatch(ctx, handler, batch)
			}

			msg.Release()

		case <-time.After(batchWaitTime):
			if batch.IsEmpty() {
				continue
			}

			ctx := context.Background()

			log.Event(ctx, "batch wait time reached. proceeding with batch", log.INFO, log.Data{"batchsize": batch.Size()})
			ProcessBatch(ctx, handler, batch)

		case eventClose := <-consumer.closing:
			log.Event(eventClose.ctx, "closing event consumer loop", log.INFO)
			close(consumer.closing)
			return
		}
	}
}

// ProcessBatch will attempt to handle and commit the batch, or shutdown if something goes horribly wrong.
func ProcessBatch(ctx context.Context, handler Handler, batch *Batch) {
	err := handler.Handle(ctx, batch.Events())
	if err != nil {
		log.Info(ctx, "error processing batch", log.Data{"err": err})
		return
	}

	//Handle Batch Events " Ends"
	end := time.Now()
	log.Info(ctx, "batch event processed - committing message - with batchsizee", log.Data{"batch end time": end, "batch-size": batch.Size()})

	batch.Commit()
}

// unmarshal converts a event instance to []byte.
func unmarshal(message kafka.Message) (*models.PublishedContentModel, error) {
	var event models.PublishedContentModel
	err := schema.PublishedContentEvent.Unmarshal(message.GetData(), &event)
	return &event, err
}
