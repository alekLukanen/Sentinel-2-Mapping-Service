package database

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


type Event struct {
	ID             primitive.ObjectID `bson:"_id"`
	UpdatedDate    primitive.DateTime `bson:"updated_date"`
	EventType      string             `bson:"event_type"`
	StartAfterDate primitive.DateTime `bson:"start_after_date"`
	StartedDate    primitive.DateTime `bson:"started_date"`
	Started        bool               `bson:"started"`
	Attempts       int                `bson:"attempts"`
	MaxAttemps     int                `bson:"max_attempts"`
	Priority       int                `bson:"priority"`
	Data           map[string]string  `bson:"data"`
	Errors         []string           `bson:"errors"`
	Passed         bool               `bson:"passed"`
	Failed         bool               `bson:"failed"`
}

func (obj *Event) ToBson(includeId bool) bson.D {
	doc :=  bson.D{
		{"updated_date", obj.UpdatedDate},
		{"event_type", obj.EventType},
		{"start_after_date", obj.StartAfterDate},
		{"started_date", obj.StartedDate},
		{"started", obj.Started},
		{"attempts", obj.Attempts},
		{"max_attempts", obj.MaxAttemps},
		{"priority", obj.Priority},
		{"data", obj.Data},
		{"errors", obj.Errors},
		{"passed", obj.Passed},
		{"failed", obj.Failed},
	}
	if includeId {
		doc = append(doc, bson.E{"_id", obj.ID})
	}
	return doc
}
func (obj *Event) RefreshFromDb(ctx context.Context, client *mongo.Client) error {
	coll := EventCollection(client)
	err := coll.FindOne(ctx, bson.D{{"_id", obj.ID}}).Decode(obj)
	return err
}

func EventCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(DatabaseName()).Collection("event")
}

func SaveEvent(ctx context.Context, client *mongo.Client, event *Event) error {
	nullId := primitive.NilObjectID
	coll := EventCollection(client)

	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	if event.ID == nullId {
		event.ID = primitive.NewObjectID()
		event.UpdatedDate = primitive.NewDateTimeFromTime(time.Now())
		_, err := coll.InsertOne(mongoCtx, event)
		return err
	} else {
		event.UpdatedDate = primitive.NewDateTimeFromTime(time.Now())
		filter := bson.D{{"_id", event.ID}}
		_, err := coll.UpdateOne(mongoCtx, filter, bson.D{{"$set", event.ToBson(false)}})
		return err
	}
}


func CountEvents(ctx context.Context, client *mongo.Client, filter bson.D) (int64, error) {
	coll := EventCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	count, err := coll.CountDocuments(mongoCtx, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}


func FindEvents(ctx context.Context, client *mongo.Client, filter bson.D, opts *options.FindOptions) (*[]Event, error) {
	coll := EventCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	events := make([]Event, 0, 100)
	cursor, err := coll.Find(mongoCtx, filter, opts)
	if err = cursor.All(mongoCtx, &events); err != nil {
		return nil, err
	}

	return &events, err
}

func FindNextEvent(ctx context.Context, client *mongo.Client) (*Event, error) {
	coll := EventCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()
	
	opts := options.FindOneAndUpdate().SetUpsert(false).SetSort(bson.D{{"priority", -1}})
	filter := bson.D{
		{"started", false},
		{"passed", false},
		{"failed", false},
		{"$expr", bson.D{
			{"$lt", bson.A{"$attempts", "$max_attempts"}},
		}},
		{"$expr", bson.D{
			{"$gt", bson.A{primitive.NewDateTimeFromTime(time.Now()), "$start_after_date"}},
		}},
	}
	update := bson.D{{"$set", bson.D{
		{"updated_date", primitive.NewDateTimeFromTime(time.Now())},
		{"started_date", primitive.NewDateTimeFromTime(time.Now())},
		{"started", true},
	}}}
	var updatedEvent Event
	err := coll.FindOneAndUpdate(
		mongoCtx,
		filter,
		update,
		opts,
	).Decode(&updatedEvent)

	return &updatedEvent, err
}

