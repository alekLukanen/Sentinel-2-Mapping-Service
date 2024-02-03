package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	db "core_service/database"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type TaskDefinition struct {
	TaskFunc    func(context.Context, *db.Event) error
	MaxDuration time.Duration
}

var TaskDefinitions = map[string]TaskDefinition{
	"RequestCurrentIndexFilesTask": TaskDefinition{TaskFunc: RequestCurrentIndexFilesTask, MaxDuration: 1 * time.Hour},
	"RequestMapTask":               TaskDefinition{TaskFunc: RequestMapTask, MaxDuration: 5 * time.Minute},
	"BuildBoundaryMapTask":         TaskDefinition{TaskFunc: BuildBoundaryMapTask, MaxDuration: 5 * time.Minute},
	"FailableTask":                 TaskDefinition{TaskFunc: FailableTask, MaxDuration: 5 * time.Second},
}

func FailableTask(ctx context.Context, event *db.Event) error {
	log.Printf("FailableTask(%s)\n", event.ID.Hex())

	if event.Data["fail"] == "true" {
		return errors.New("failed task!")
	}
	return nil
}

func WorkerClient(ctx context.Context) {
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// background task
	// go StartObserver(ctx, dbClient)

	for {
		log.Println("[worker] processing next event")
		delay := 5 * time.Second
		if err := ProcessNextEvent(ctx, dbClient); err != nil {
			if err == mongo.ErrNoDocuments {
				log.Println("[worker] no events found")
			} else {
				log.Println(err)
			}
		} else {
			delay = 1 * time.Millisecond
		}

		// might not be needed due to db.FindNextEvent failing when no events
		time.Sleep(delay)
	}
}

func ProcessNextEvent(ctx context.Context, dbClient *mongo.Client) error {
	event, err := db.FindNextEvent(ctx, dbClient)
	if err != nil {
		return err
	}

	log.Println("[worker] processing event:", event.ID.Hex(), ", type:", event.EventType)

	task, exists := TaskDefinitions[event.EventType]
	if !exists {
		log.Println("[worker] event type not implemented")
		event.Failed = true
		if err := db.SaveEvent(ctx, dbClient, event); err != nil {
			return err
		}
	}

	taskCtx, taskCtxCancel := context.WithTimeout(ctx, task.MaxDuration)
	defer taskCtxCancel()

	err = task.TaskFunc(taskCtx, event)
	event.Attempts += 1
	if err != nil {
		errorData := fmt.Sprintf("%v", err)
		if len(errorData) > 250 {
			errorData = errorData[:250]
		}
		event.Errors = append(event.Errors, errorData)
		event.Started = false
		if event.Attempts >= event.MaxAttemps {
			event.Failed = true
		}
	} else {
		event.Passed = true
	}

	if err := db.SaveEvent(ctx, dbClient, event); err != nil {
		return err
	}

	return nil
}

func StartObserver(ctx context.Context, dbClient *mongo.Client) {
	indexTicker := time.NewTicker(24 * time.Hour)

	for {
		select {
		case <-indexTicker.C:
			if err := PublishPeriodicIndexEvent(ctx, dbClient); err != nil {
				log.Println(err)
			}
		}
	}

}

func PublishPeriodicIndexEvent(ctx context.Context, dbClient *mongo.Client) error {

	// handle periodic load of new satellite data
	activeIndexEventCount, err := db.CountEvents(
		ctx,
		dbClient,
		bson.D{
			{"event_type", "RequestCurrentIndexFilesTask"},
			{"started", false},
			{"passed", false},
			{"failed", false},
			{"$expr", bson.D{
				{"$lt", bson.A{"$attempts", "$max_attempts"}},
			}},
		},
	)
	if err != nil {
		return err
	} else if activeIndexEventCount == 0 {
		log.Println("[observer] publishing index file event")
		indexFileEvent := db.Event{
			EventType:      "RequestCurrentIndexFilesTask",
			MaxAttemps:     1,
			StartAfterDate: primitive.NewDateTimeFromTime(time.Now().Add(7 * 24 * time.Hour)),
		}
		if err := db.SaveEvent(ctx, dbClient, &indexFileEvent); err != nil {
			return err
		}
	} else {
		log.Println("[observer] index file event not published")
	}

	return nil
}
