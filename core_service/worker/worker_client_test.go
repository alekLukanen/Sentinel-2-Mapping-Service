package worker

import (
	"context"
	"testing"
	"time"

	db "core_service/database"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func TestPublishPeriodicIndexEvent(t *testing.T) {
	db.CleanTestDatabase()

	ctx := context.Background()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if eventCount, err := db.CountEvents(ctx, dbClient, bson.D{}); err != nil {
		t.Fatal(err)
	} else if eventCount != 0 {
		t.Fatal("event collection was not empty")
	}

	if err := PublishPeriodicIndexEvent(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	events, err := db.FindEvents(ctx, dbClient, bson.D{}, &options.FindOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(*events) != 1 {
		t.Fatalf("expected 1 event but found %d", len(*events))
	}

	event1 := (*events)[0]
	if event1.EventType != "RequestCurrentIndexFilesTask" || event1.MaxAttemps != 1 || event1.StartAfterDate == 0 {
		t.Log("event not created as expected")
		t.Fatal(event1)
	}

	// attempt to publish another event
	if err := PublishPeriodicIndexEvent(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	events, err = db.FindEvents(ctx, dbClient, bson.D{}, &options.FindOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(*events) != 1 {
		t.Fatalf("expected 1 event but found %d", len(*events))
	}

	event2 := (*events)[0]
	if event1.ID != event2.ID {
		t.Fatal("the orignal event was deleted and replaced")
	}
	if event2.EventType != "RequestCurrentIndexFilesTask" || event2.MaxAttemps != 1 || event2.StartAfterDate == 0 {
		t.Log("event was updated unexpectedly")
		t.Fatal(event2)
	}

}


func TestProcessNextEventWithPassingEvent(t *testing.T) {
	db.CleanTestDatabase()

	ctx := context.Background()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	event1 := db.Event{
		EventType: "FailableTask",
		MaxAttemps: 1,
		StartAfterDate: primitive.NewDateTimeFromTime(time.Now().Add(0*time.Second)),
		Data: map[string]string{"fail": "false"},
	}
	err = db.SaveEvent(ctx, dbClient, &event1)
	if err != nil {
		t.Fatal(err)
	}

	if err = ProcessNextEvent(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if err = event1.RefreshFromDb(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if event1.Attempts != 1 || !event1.Passed || event1.Failed {
		t.Fatal("event was not passed correctly")
	} else if event1.Data["fail"] != "false" {
		t.Fatal("data was changed")
	}

	if err = ProcessNextEvent(ctx, dbClient); err == nil {
		t.Fatal("should have returned an error")
	} else if err != mongo.ErrNoDocuments {
		t.Log("the error is not correct")
	}

}

func TestProcessNextEventWithFailingEvent(t *testing.T) {
	db.CleanTestDatabase()

	ctx := context.Background()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	event1 := db.Event{
		EventType: "FailableTask",
		MaxAttemps: 1,
		StartAfterDate: primitive.NewDateTimeFromTime(time.Now().Add(0*time.Second)),
		Data: map[string]string{"fail": "true"},
	}
	err = db.SaveEvent(ctx, dbClient, &event1)
	if err != nil {
		t.Fatal(err)
	}

	if err = ProcessNextEvent(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if err = event1.RefreshFromDb(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if event1.Attempts != 1 || event1.Passed || !event1.Failed {
		t.Fatal("event was not failed correctly")
	} else if event1.Data["fail"] != "true" {
		t.Fatal("data was changed")
	} else if len(event1.Errors) != 1 || event1.Errors[0] != "failed task!" {
		t.Fatal(event1.Errors[0])
	}

	if err = ProcessNextEvent(ctx, dbClient); err == nil {
		t.Fatal("should have returned an error")
	} else if err != mongo.ErrNoDocuments {
		t.Log("the error is not correct")
	}

}