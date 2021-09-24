package handler

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/v2/log"
)

// PublishedContentHandler ...
type PublishedContentHandler struct {
}

// Handle takes a single event.
func (h *PublishedContentHandler) Handle(ctx context.Context, cfg *config.Config, event *models.PublishedContentExtracted) (err error) {
	logData := log.Data{
		"event": event,
	}
	log.Info(ctx, "event handler called", logData)

	success := fmt.Sprintf("Published Content Data Type, %s!", event.DataType)
	err = ioutil.WriteFile(cfg.OutputFilePath, []byte(success), 0644)
	if err != nil {
		return err
	}

	logData["success"] = success
	log.Info(ctx, "event successfully handled", logData)

	return nil
}
