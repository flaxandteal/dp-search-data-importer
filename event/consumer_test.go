package event_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/event/eventtest"
	"github.com/ONSdigital/dp-search-data-importer/models"

	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/event"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testCtx = context.Background()

	expectedEvent = models.SearchDataImportModel{
		DataType:        "testDataType",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword1", "testkeyword2"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "",
		TraceID:         "",
	}
)

func TestConsumeWithOneMessage(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {

		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Log(ctx, "failed to retrieve configuration", err)
			t.Fail()
		}

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {

			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)
			messageConsumer.Channels().Upstream <- message

			<-eventHandler.EventUpdated

			Convey("The expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 1)

				event := eventHandler.Events[0]
				So(event.DataType, ShouldEqual, expectedEvent.DataType)
				So(event.Title, ShouldEqual, expectedEvent.Title)
			})
			Convey("The message is committed and the consumer is released", func() {
				<-message.UpstreamDone()
				So(len(message.CommitCalls()), ShouldEqual, 1)
				So(len(message.ReleaseCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestClose(t *testing.T) {

	Convey("Given a consumer", t, func() {
		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Log(ctx, "failed to retrieve configuration", err)
			t.Fail()
		}
		consumer := event.NewConsumer()
		go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

		Convey("When close is called", func() {
			err := consumer.Close(nil)

			Convey("The expected event is sent to the handler", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
