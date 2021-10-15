package steps

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/ONSdigital/dp-search-data-importer/service"
	"github.com/cucumber/godog"
	"github.com/rdumont/assistdog"
	"github.com/stretchr/testify/assert"
)

func (c *Component) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^these published contents are consumed:$`, c.theseSearchImportEventsAreConsumed)
	ctx.Step(`^I should receive a published-content response$`, c.iShouldReceiveAPublishedContentResponse)
}

func (c *Component) iShouldReceiveAPublishedContentResponse() error {
	content, err := ioutil.ReadFile(c.cfg.OutputFilePath)
	if err != nil {
		return err
	}

	assert.Equal(c, "Search Data Import, Tim!", string(content))

	return c.StepError()
}

func (c *Component) theseSearchImportEventsAreConsumed(table *godog.Table) error {

	SearchDataImportEvents, err := c.convertToSearchDataImportEvents(table)
	if err != nil {
		return err
	}

	signals := registerInterrupt()

	// run application in separate goroutine
	go func() {
		c.svc, err = service.Run(context.Background(), c.serviceList, "", "", "", c.errorChan)
	}()

	// consume extracted SearchDataImportEvents
	for _, e := range SearchDataImportEvents {
		if err := c.sendToConsumer(e); err != nil {
			return err
		}
	}

	time.Sleep(300 * time.Millisecond)

	// kill application
	signals <- os.Interrupt

	return nil
}

func (c *Component) convertToSearchDataImportEvents(table *godog.Table) ([]*models.SearchDataImportModel, error) {
	assist := assistdog.NewDefault()
	events, err := assist.CreateSlice(&models.SearchDataImportModel{}, table)
	if err != nil {
		return nil, err
	}
	return events.([]*models.SearchDataImportModel), nil
}

func (c *Component) sendToConsumer(e *models.SearchDataImportModel) error {
	bytes, err := schema.SearchDataImportEvent.Marshal(e)
	if err != nil {
		return err
	}

	c.KafkaConsumer.Channels().Upstream <- kafkatest.NewMessage(bytes, 0)
	return nil

}

func registerInterrupt() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	return signals
}
