package eventtest

import (
	"context"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/log"
)

var _ event.Handler = (*EventHandler)(nil)

// NewEventHandler returns a new mock event handler to capture event
func NewEventHandler() *EventHandler {

	events := make([]*models.PublishedContentModel, 0)
	eventUpdated := make(chan bool)

	return &EventHandler{
		Events:       events,
		EventUpdated: eventUpdated,
	}
}

// EventHandler provides a mock implementation that captures events to check.
type EventHandler struct {
	Events       []*models.PublishedContentModel
	Error        error
	EventUpdated chan bool
}

// Handle captures the given event and stores it for later assertions
func (handler *EventHandler) Handle(ctx context.Context, events []*models.PublishedContentModel) error {
	log.Event(ctx, "handle called", log.INFO)
	handler.Events = events

	handler.EventUpdated <- true
	return handler.Error
}
