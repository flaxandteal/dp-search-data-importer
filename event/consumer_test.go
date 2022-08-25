package event_test

import (
	"context"
	"testing"
	"time"

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
		Topics:          []string{""},
		TraceID:         "",
	}

	expectedEventWithEmptyTopicString = models.SearchDataImportModel{
		DataType:        "testDataType3",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword31", "testkeyword32"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "",
		Topics:          []string{""},
		TraceID:         "",
	}

	expectedEventWithMissingTopicArray = models.SearchDataImportModel{
		DataType:        "testDataType3",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword31", "testkeyword32"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "",
		TraceID:         "",
	}

	expectedEventWithEmptyTopicArray = models.SearchDataImportModel{
		DataType:        "testDataType3",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword31", "testkeyword32"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "",
		Topics:          []string{},
		TraceID:         "",
	}

	expectedEvent2 = models.SearchDataImportModel{
		DataType:        "testDataType2",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword21", "testkeyword22"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "",
		Topics:          []string{"testtopic1", "testtopic2"},
		TraceID:         "",
	}
)

func TestConsumeWithOneMessage(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {

		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to retrieve configuration: %v", err)
		}

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {

			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message := kafkatest.NewMessage([]byte(marshal(expectedEvent1)), 0)
			messageConsumer.Channels().Upstream <- message

			<-eventHandler.EventUpdated
			consumer.Close(testCtx)

			Convey("Then the expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 1)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEvent1.DataType)
				So(actual.Title, ShouldEqual, expectedEvent1.Title)
				So(len(actual.Topics), ShouldEqual, 1)
			})
			Convey("And the message is committed and the consumer is released", func() {
				<-consumer.Closed
				So(len(message.CommitAndReleaseCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestConsumeWithEmptyTopicString(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {

		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to retrieve configuration: %v", err)
		}

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {

			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message := kafkatest.NewMessage([]byte(marshal(expectedEventWithEmptyTopicString)), 0)
			messageConsumer.Channels().Upstream <- message

			<-eventHandler.EventUpdated
			consumer.Close(testCtx)

			Convey("Then the expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 1)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEventWithEmptyTopicString.DataType)
				So(actual.Title, ShouldEqual, expectedEventWithEmptyTopicString.Title)
				So(len(actual.Topics), ShouldEqual, 1)
			})
			Convey("And the message is committed and the consumer is released", func() {
				<-consumer.Closed
				So(len(message.CommitAndReleaseCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestConsumeWithMissingTopicElement(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {

		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to retrieve configuration: %v", err)
		}

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {

			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message := kafkatest.NewMessage([]byte(marshal(expectedEventWithMissingTopicArray)), 0)
			messageConsumer.Channels().Upstream <- message

			<-eventHandler.EventUpdated
			consumer.Close(testCtx)

			Convey("Then the expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 1)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEventWithMissingTopicArray.DataType)
				So(actual.Title, ShouldEqual, expectedEventWithMissingTopicArray.Title)
				So(len(actual.Topics), ShouldEqual, 0)
			})
			Convey("And the message is committed and the consumer is released", func() {
				<-consumer.Closed
				So(len(message.CommitAndReleaseCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestConsumeWithEmptyTopicArray(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {

		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to retrieve configuration: %v", err)
		}

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {

			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message := kafkatest.NewMessage([]byte(marshal(expectedEventWithEmptyTopicArray)), 0)
			messageConsumer.Channels().Upstream <- message

			<-eventHandler.EventUpdated
			consumer.Close(testCtx)

			Convey("Then the expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 1)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEventWithEmptyTopicArray.DataType)
				So(actual.Title, ShouldEqual, expectedEventWithEmptyTopicArray.Title)
				So(len(actual.Topics), ShouldEqual, 0)
			})
			Convey("And the message is committed and the consumer is released", func() {
				<-consumer.Closed
				So(len(message.CommitAndReleaseCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestConsumeWithTwoMessages(t *testing.T) {

	Convey("Given a consumer with a mocked message producer with an expected message", t, func() {
		messageConsumer := kafkatest.NewMessageConsumer(false)
		eventHandler := eventtest.NewEventHandler()
		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to retrieve configuration: %v", err)
		}
		cfg.BatchWaitTime = time.Second * 2
		cfg.BatchSize = 2

		consumer := event.NewConsumer()

		Convey("When consume is called", func() {
			go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

			message1 := kafkatest.NewMessage([]byte(marshal(expectedEvent1)), 0)
			messageConsumer.Channels().Upstream <- message1

			message2 := kafkatest.NewMessage([]byte(marshal(expectedEvent2)), 0)
			messageConsumer.Channels().Upstream <- message2

			<-eventHandler.EventUpdated
			consumer.Close(testCtx)

			Convey("The expected event is sent to the handler", func() {
				So(len(eventHandler.Events), ShouldEqual, 2)

				actual := eventHandler.Events[0]
				So(actual.DataType, ShouldEqual, expectedEvent1.DataType)
				So(actual.Title, ShouldEqual, expectedEvent1.Title)
			})
			Convey("The message is committed and the consumer is released", func() {
				<-consumer.Closed
				So(len(message2.CommitAndReleaseCalls()), ShouldEqual, 1)
				So(len(message1.CommitAndReleaseCalls()), ShouldEqual, 1)
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
			t.Fatalf("failed to retrieve configuration: %v", err)
		}
		consumer := event.NewConsumer()
		go consumer.Consume(testCtx, messageConsumer, eventHandler, cfg)

		Convey("When close is called", func() {
			err := consumer.Close(testCtx)

			Convey("The expected event is sent to the handler", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
