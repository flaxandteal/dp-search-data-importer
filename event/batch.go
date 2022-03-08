package event

import (
	"context"
	"errors"

	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/ONSdigital/log.go/v2/log"
)

// Batch handles adding raw messages to a batch of SearchDataImportModel events.
type Batch struct {
	maxSize  int
	events   []*models.SearchDataImportModel
	messages []Message
}

// Message represents a single message to be added to the batch.
type Message interface {
	GetData() []byte
	Mark()
	Commit()
}

// NewBatch returns a new batch instance of the given size.
func NewBatch(batchSize int) *Batch {
	events := make([]*models.SearchDataImportModel, 0, batchSize)

	return &Batch{
		maxSize: batchSize,
		events:  events,
	}
}

// Add a message to the batch.
func (batch *Batch) Add(ctx context.Context, message Message) (err error) {

	event, err := Unmarshal(message)
	if err != nil {
		log.Error(ctx, "failed to unmarshal event", err)
		return errors.New("failed to unmarshal event while adding an event to batch")
	}

	ctx = dprequest.WithRequestId(ctx, event.TraceID)
	log.Info(ctx, "event received to be added into the batch", log.Data{"traceid": event.TraceID})
	batch.messages = append(batch.messages, message)
	batch.events = append(batch.events, event)
	return
}

// Size returns the number of events currently in the batch.
func (batch *Batch) Size() int {
	return len(batch.events)
}

// IsFull returns true if the batch is full based on the configured maxSize.
func (batch *Batch) IsFull() bool {
	return len(batch.events) == batch.maxSize
}

// Events returns the events currenty in the batch.
func (batch *Batch) Events() []*models.SearchDataImportModel {
	return batch.events
}

// IsEmpty returns true if the batch has no events in it.
func (batch *Batch) IsEmpty() bool {
	return len(batch.events) == 0
}

// Commit is called when the batch has been processed. The last message has been released already, so at this point we just need to commit
func (batch *Batch) Commit() {
	for i, msg := range batch.messages {
		if i < len(batch.messages)-1 {
			msg.Mark()
			continue
		}
		msg.Commit()
	}
	batch.Clear()
}

// Clear will reset to batch to contain no events.
func (batch *Batch) Clear() {
	batch.events = batch.events[0:0]
	batch.messages = batch.messages[0:0]
}

// Unmarshal converts an event instance to []byte.
func Unmarshal(message Message) (*models.SearchDataImportModel, error) {
	var event models.SearchDataImportModel
	err := schema.SearchDataImportEvent.Unmarshal(message.GetData(), &event)
	return &event, err
}
