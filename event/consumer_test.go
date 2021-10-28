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

	expectedEvent1 = models.SearchDataImportModel{
		DataType:        "testDataType1",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword1", "testkeyword2"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "testTitle1",
		TraceID:         "",
	}

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

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEvent.DataType)
				So(actual.Title, ShouldEqual, expectedEvent.Title)
			})
			Convey("The message is committed and the consumer is released", func() {
				<-message.UpstreamDone()
				So(len(message.CommitCalls()), ShouldEqual, 1)
				So(len(message.ReleaseCalls()), ShouldEqual, 1)
			})
		})
		consumer.Close(nil)
	})
}

func TestConsumeWithFullBatchSizeMessage(t *testing.T) {
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

			message1 := kafkatest.NewMessage([]byte(marshal(expectedEvent1)), 0)
			messageConsumer.Channels().Upstream <- message1

			message2 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)
			messageConsumer.Channels().Upstream <- message2

			message3 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)
			messageConsumer.Channels().Upstream <- message3

			message4 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)
			messageConsumer.Channels().Upstream <- message4

			<-eventHandler.EventUpdated

			Convey("The expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 4)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEvent1.DataType)
				So(actual.Title, ShouldEqual, expectedEvent1.Title)
			})
			Convey("The message is committed and the consumer is released", func() {
				<-message1.UpstreamDone()
				So(len(message4.CommitCalls()), ShouldEqual, 1)
				So(len(message4.ReleaseCalls()), ShouldEqual, 1)
			})
		})
		consumer.Close(nil)
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
