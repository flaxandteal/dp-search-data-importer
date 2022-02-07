package event_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	. "github.com/smartystreets/goconvey/convey"
)

var ctx = context.Background()

func TestIsEmpty(t *testing.T) {
	Convey("Given a batch", t, func() {

		expectedEvent := getExpectedEvent()
		message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When the batch has no messages added", func() {
			Convey("Then the batch is empty", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
			})
		})

		Convey("When the batch has a message added", func() {

			err := batch.Add(ctx, message)
			Convey("Then the batch is not empty", func() {
				So(batch.IsEmpty(), ShouldBeFalse)

				Convey("And no error is returned", func() {
					So(err, ShouldBeNil)
				})
			})
		})
	})
}

func TestAdd(t *testing.T) {

	Convey("Given a batch", t, func() {

		expectedEvent := getExpectedEvent()
		message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When add is called with a valid message", func() {

			err := batch.Add(ctx, message)
			So(err, ShouldBeNil)

			Convey("Then the batch contains the expected event.", func() {
				So(batch.Size(), ShouldEqual, 1)
				So(batch.Events()[0].DataType, ShouldEqual, expectedEvent.DataType)
			})
		})
	})
}

func TestCommitWithOneMessage(t *testing.T) {

	Convey("Given a batch with one valid messages", t, func() {

		expectedEvent := getExpectedEvent()
		message1 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		err := batch.Add(ctx, message1)
		So(err, ShouldBeNil)

		Convey("When commit is called", func() {

			batch.Commit()

			Convey("Then a messages that is present in batch are marked and committed", func() {
				So(message1.IsMarked(), ShouldBeTrue)
				So(message1.IsCommitted(), ShouldBeTrue)
			})

			Convey("And the batch is emptied.", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 0)
			})
		})
	})
}

func TestCommitWithTwoMessage(t *testing.T) {

	Convey("Given a batch with two valid messages", t, func() {

		expectedEvent := getExpectedEvent()
		message1 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)
		message2 := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 2
		batch := event.NewBatch(batchSize)

		err := batch.Add(ctx, message1)
		So(err, ShouldBeNil)
		err2 := batch.Add(ctx, message2)
		So(err2, ShouldBeNil)

		Convey("When commit is called", func() {

			batch.Commit()

			Convey("Then all messages that were present in batch are marked, and last one is committed", func() {
				So(message1.IsMarked(), ShouldBeTrue)
				So(message2.IsMarked(), ShouldBeTrue)
				So(message1.IsCommitted(), ShouldBeFalse)
				So(message2.IsCommitted(), ShouldBeTrue)
			})

			Convey("And the batch is emptied.", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 0)
			})

			Convey("And the batch can be reused", func() {
				err1 := batch.Add(ctx, message1)
				So(err1, ShouldBeNil)

				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 1)

				So(batch.Events()[0].DataType, ShouldEqual, expectedEvent.DataType)

				err2 := batch.Add(ctx, message2)
				So(err2, ShouldBeNil)
				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeTrue)
				So(batch.Size(), ShouldEqual, 2)

				So(batch.Events()[1].Title, ShouldEqual, expectedEvent.Title)
			})
		})
	})
}

func TestSize(t *testing.T) {

	Convey("Given a batch", t, func() {

		expectedEvent := getExpectedEvent()
		message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		So(batch.Size(), ShouldEqual, 0)

		Convey("When add is called with a valid message", func() {

			err := batch.Add(ctx, message)
			So(err, ShouldBeNil)

			Convey("Then the batch size should increase.", func() {
				So(batch.Size(), ShouldEqual, 1)
				err = batch.Add(ctx, message)
				So(err, ShouldBeNil)
				So(batch.Size(), ShouldEqual, 2)
			})
		})
	})
}

func TestIsFull(t *testing.T) {

	Convey("Given a batch with a size of 2", t, func() {

		expectedEvent := getExpectedEvent()
		message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		batchSize := 2
		batch := event.NewBatch(batchSize)

		So(batch.IsFull(), ShouldBeFalse)

		Convey("When the number of messages added equals the batch size", func() {

			err1 := batch.Add(ctx, message)
			So(err1, ShouldBeNil)
			So(batch.IsFull(), ShouldBeFalse)

			err2 := batch.Add(ctx, message)
			So(err2, ShouldBeNil)

			Convey("Then the batch should be full.", func() {
				So(batch.IsFull(), ShouldBeTrue)
			})
		})
	})
}

func TestToEvent(t *testing.T) {

	Convey("Given a event schema encoded using avro", t, func() {

		expectedEvent := getExpectedEvent()
		message := kafkatest.NewMessage([]byte(marshal(expectedEvent)), 0)

		Convey("When the expectedEvent is unmarshalled", func() {

			testEvent, err := event.Unmarshal(message)

			Convey("Then the expectedEvent has the expected values", func() {
				So(err, ShouldBeNil)
				So(testEvent.DataType, ShouldEqual, expectedEvent.DataType)
			})
		})
	})
}

// Marshal helper method to marshal a event into a []byte
func marshal(smEvent models.SearchDataImportModel) []byte {
	bytes, err := schema.SearchDataImportEvent.Marshal(smEvent)
	So(err, ShouldBeNil)
	return bytes
}

func getExpectedEvent() models.SearchDataImportModel {
	expectedEvent := models.SearchDataImportModel{
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
		Topics:          []string{"testtopic1", "testtopic2"},
		TraceID:         "",
	}
	return expectedEvent
}
