package event_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	. "github.com/smartystreets/goconvey/convey"
)

var ctx = context.Background()

func TestIsEmpty(t *testing.T) {

	Convey("Given a batch that is not empty", t, func() {

		expectedEvent := getExampleEvent()
		message := kafkatest.NewMessage([]byte(marshal(*expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When the batch has no messages added", func() {
			Convey("The batch is empty", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
			})
		})

		Convey("When the batch has a message added", func() {

			batch.Add(ctx, message)

			Convey("The batch is not empty", func() {
				So(batch.IsEmpty(), ShouldBeFalse)
			})
		})
	})
}

func TestAdd(t *testing.T) {

	Convey("Given a batch", t, func() {

		expectedEvent := getExampleEvent()
		message := kafkatest.NewMessage([]byte(marshal(*expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		Convey("When add is called with a valid message", func() {

			batch.Add(ctx, message)

			Convey("The batch contains the expected event.", func() {
				So(batch.Size(), ShouldEqual, 1)
				So(batch.Events()[0].RecipientName, ShouldEqual, expectedEvent.RecipientName)
			})
		})
	})
}

func TestCommit(t *testing.T) {

	Convey("Given a batch with two valid messages", t, func() {

		expectedEvent1 := event.PublishedContentExtracted{RecipientName: "Test"}
		expectedEvent2 := event.PublishedContentExtracted{RecipientName: "Test"}
		expectedEvent3 := event.PublishedContentExtracted{RecipientName: "Test"}
		expectedEvent4 := event.PublishedContentExtracted{RecipientName: "Test"}
		message1 := kafkatest.NewMessage([]byte(marshal(expectedEvent1)), 0)
		message2 := kafkatest.NewMessage([]byte(marshal(expectedEvent2)), 0)
		message3 := kafkatest.NewMessage([]byte(marshal(expectedEvent3)), 0)
		message4 := kafkatest.NewMessage([]byte(marshal(expectedEvent4)), 0)

		batchSize := 2
		batch := event.NewBatch(batchSize)

		batch.Add(ctx, message1)
		batch.Add(ctx, message2)

		batch.Commit()

		Convey("When commit is called", func() {

			Convey("All messages that were present in batch are marked, and last one is committed", func() {
				So(message1.IsMarked(), ShouldBeTrue)
				So(message2.IsMarked(), ShouldBeTrue)
				So(message2.IsCommitted(), ShouldBeTrue)
			})

			Convey("The batch is emptied.", func() {
				So(batch.IsEmpty(), ShouldBeTrue)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 0)
			})

			Convey("The batch can be reused", func() {
				batch.Add(ctx, message3)

				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeFalse)
				So(batch.Size(), ShouldEqual, 1)

				So(batch.Events()[0].RecipientName, ShouldEqual, expectedEvent3.RecipientName)

				batch.Add(ctx, message4)

				So(batch.IsEmpty(), ShouldBeFalse)
				So(batch.IsFull(), ShouldBeTrue)
				So(batch.Size(), ShouldEqual, 2)

				So(batch.Events()[1].RecipientName, ShouldEqual, expectedEvent4.RecipientName)
			})
		})
	})
}

func TestSize(t *testing.T) {

	Convey("Given a batch", t, func() {

		expectedEvent := getExampleEvent()
		message := kafkatest.NewMessage([]byte(marshal(*expectedEvent)), 0)

		batchSize := 1
		batch := event.NewBatch(batchSize)

		So(batch.Size(), ShouldEqual, 0)

		Convey("When add is called with a valid message", func() {

			batch.Add(ctx, message)

			Convey("The batch size should increase.", func() {
				So(batch.Size(), ShouldEqual, 1)
				batch.Add(ctx, message)
				So(batch.Size(), ShouldEqual, 2)
			})
		})
	})
}

func TestIsFull(t *testing.T) {

	Convey("Given a batch with a size of 2", t, func() {

		expectedEvent := getExampleEvent()
		message := kafkatest.NewMessage([]byte(marshal(*expectedEvent)), 0)

		batchSize := 2
		batch := event.NewBatch(batchSize)

		So(batch.IsFull(), ShouldBeFalse)

		Convey("When the number of messages added equals the batch size", func() {

			batch.Add(ctx, message)
			So(batch.IsFull(), ShouldBeFalse)
			batch.Add(ctx, message)

			Convey("The batch should be full.", func() {
				So(batch.IsFull(), ShouldBeTrue)
			})
		})
	})
}

func TestToEvent(t *testing.T) {

	Convey("Given a event schema encoded using avro", t, func() {

		expectedEvent := getExampleEvent()
		message := kafkatest.NewMessage([]byte(marshal(*expectedEvent)), 0)

		Convey("When the expectedEvent is unmarshalled", func() {

			event, err := event.Unmarshal(message)

			Convey("The expectedEvent has the expected values", func() {
				So(err, ShouldBeNil)
				So(event.RecipientName, ShouldEqual, expectedEvent.RecipientName)
			})
		})
	})
}

// Marshal helper method to marshal a event into a []byte
func marshal(event event.PublishedContentExtracted) []byte {
	bytes, err := schema.PublishedContentEvent.Marshal(event)
	So(err, ShouldBeNil)
	return bytes
}

func getExampleEvent() *event.PublishedContentExtracted {
	expectedEvent := &event.PublishedContentExtracted{
		RecipientName: "1234",
	}
	return expectedEvent
}
