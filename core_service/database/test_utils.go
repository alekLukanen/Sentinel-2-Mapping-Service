package database

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)


func CleanTestDatabase() {
	log.Println("cleaning database")
	if ENVIRONMENT != "local" {
		panic("not running on local machine")
	}

	ctx := context.Background()
	if err := Configure(ctx); err != nil {
		panic(err)
	}

	dbClient, err := DefaultDatabaseClient(ctx)
	if err != nil {
		panic(err)
	}

	objectStoreColl := dbClient.Database("test_db").Collection("object_store")
	settingColl := dbClient.Database("test_db").Collection("setting")
	tileColl := dbClient.Database("test_db").Collection("tile")
	boundaryColl := dbClient.Database("test_db").Collection("boundary")
	eventColl := dbClient.Database("test_db").Collection("event")
	rasterColl := dbClient.Database("test_db").Collection("raster")

	collectionsToClean := []*mongo.Collection{
		objectStoreColl, 
		settingColl, 
		tileColl, 
		boundaryColl, 
		eventColl, 
		rasterColl,
	}
	mongoCtx, _ := context.WithTimeout(ctx, 3*time.Second)
	for _, coll := range collectionsToClean {
		_, err := coll.DeleteMany(mongoCtx, bson.M{})
		if err != nil {
			panic(err)
		}
	}

}