package eventtest

import (
	"context"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/v2/log"
)

var _ event.Handler = (*EventHandler)(nil)

// NewEventHandler returns a new mock event handler to capture event
func NewEventHandler() *EventHandler {

	events := make([]*models.SearchDataImportModel, 0)
	eventUpdated := make(chan bool)

	return &EventHandler{
		Events:       events,
		EventUpdated: eventUpdated,
	}
}

// EventHandler provides a mock implementation that captures events to check.
type EventHandler struct {
	Events       []*models.SearchDataImportModel
	Error        error
	EventUpdated chan bool
}

// Handle captures the given event and stores it for later assertions
func (handler *EventHandler) Handle(ctx context.Context, cfg *config.Config, events []*models.SearchDataImportModel) error {
	log.Info(ctx, "eventtest handler called")
	handler.Events = events

	handler.EventUpdated <- true
	return handler.Error
}
