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

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When the batch has no messages added", func() {
			Convey("Then the batch is empty", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
			})
		})

		Convey("When the batch has a message added", func() {

			batch.Add(ctx, &expectedEvent)
			Convey("Then the batch is not empty", func() {
				So(batch.IsEmpty(), ShouldBeFalse)
			})
		})
	})
}

func TestAdd(t *testing.T) {

	Convey("Given a batch", t, func() {

		expectedEvent := getExpectedEvent()

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When add is called with a valid message", func() {
			batch.Add(ctx, &expectedEvent)

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

		batchSize := 1
		batch := event.NewBatch(batchSize)

		batch.Add(ctx, &expectedEvent)

		Convey("When commit is called", func() {
			batch.Commit()

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

		batchSize := 2
		batch := event.NewBatch(batchSize)

		batch.Add(ctx, &expectedEvent)
		batch.Add(ctx, &expectedEvent)

		Convey("When commit is called", func() {

			batch.Commit()

			Convey("And the batch is emptied.", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 0)
			})

			Convey("And the batch can be reused", func() {
				batch.Add(ctx, &expectedEvent)

				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 1)

				So(batch.Events()[0].DataType, ShouldEqual, expectedEvent.DataType)

				batch.Add(ctx, &expectedEvent)
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

		batchSize := 1
		batch := event.NewBatch(batchSize)

		So(batch.Size(), ShouldEqual, 0)

		Convey("When add is called with a valid message", func() {

			batch.Add(ctx, &expectedEvent)

			Convey("Then the batch size should increase.", func() {
				So(batch.Size(), ShouldEqual, 1)
				batch.Add(ctx, &expectedEvent)
				So(batch.Size(), ShouldEqual, 2)
			})
		})
	})
}

func TestIsFull(t *testing.T) {

	Convey("Given a batch with a size of 2", t, func() {

		expectedEvent := getExpectedEvent()

		batchSize := 2
		batch := event.NewBatch(batchSize)

		So(batch.IsFull(), ShouldBeFalse)

		Convey("When the number of messages added equals the batch size", func() {

			batch.Add(ctx, &expectedEvent)
			So(batch.IsFull(), ShouldBeFalse)

			batch.Add(ctx, &expectedEvent)

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
