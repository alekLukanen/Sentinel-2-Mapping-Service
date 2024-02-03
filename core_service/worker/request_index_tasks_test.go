package worker

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	db "core_service/database"
	satData "core_service/satelliteS3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)


func TestRequestCurrentIndexFilesTask(t *testing.T) {
	// upload a manifest.json file with one index.csv.gz file
	// the process should build tiles: 39PUL, 18QZG
	db.CleanTestDatabase()

	ctx := context.Background()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	objectSession, err := satData.S3Session(ctx, satData.SATELLITE_S3_INVENTORY_BUCKET)
	if err != nil {
		t.Fatal(err)
	}
	uploader := manager.NewUploader(objectSession)

	// upload manifest and index files
	manifestFile, err := os.Open("example_data/small-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	defer manifestFile.Close()

	dateKey := UTCFormattedDateOptions("")[0]
	manifestKey := fmt.Sprintf("/sentinel-cogs/sentinel-cogs/%s/manifest.json", dateKey)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_INVENTORY_BUCKET),
		Key:    aws.String(manifestKey),
		Body:   manifestFile,
	})
	if err != nil {
		// Print the error and exit.
		t.Fatalf("Unable to upload: %v", err)
	}

	indexFile, err := os.Open("example_data/small-index.csv.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer indexFile.Close()

	indexKey := "sentinel-cogs/sentinel-cogs/data/uuid4-here.csv.gz"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_INVENTORY_BUCKET),
		Key:    aws.String(indexKey),
		Body:   indexFile,
	})
	if err != nil {
		// Print the error and exit.
		t.Fatalf("Unable to upload: %v", err)
	}

	// add settings to database
	location, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}
	setting := db.Setting{
		UtmZones: []string{"39P", "18Q"},
		TileFiles: []string{"B04.tif", "B08.tif"},
		TileStartDate: primitive.NewDateTimeFromTime(time.Date(2018, time.Month(1), 1, 0, 0, 0, 0, location)),
	}
	err = db.SaveSetting(ctx, dbClient, &setting)
	if err != nil {
		t.Fatal(err)
	}

	// now ensure that the index file task creates the tiles
	// and distributes the tile download events
	if taskErr := RequestCurrentIndexFilesTask(ctx, &db.Event{}); taskErr != nil {
		t.Fatal(err)
	}

	tileColl := db.TileCollection(dbClient)
	if tileCount, err := tileColl.CountDocuments(ctx, bson.D{{}}); err != nil || tileCount != 2 {
		t.Errorf("expected only 2 documents but found %d", tileCount)
	}

	var tile1 db.Tile
	if err = tileColl.FindOne(ctx, bson.D{{"mgrs_code", "39PUL"}}).Decode(&tile1); err != nil {
		if err == mongo.ErrNoDocuments {
			t.Error("could not find tile in database")
		}
		t.Fatal("failed getting tile")
	} else if (
		tile1.Date != primitive.NewDateTimeFromTime(time.Date(2019, time.Month(9), 14, 0, 0, 0, 0, location)) || 
		tile1.SourceSatellite != "S2A-L2A" || 
		len(tile1.Files) != 0) {
		t.Fatalf("tile1 not correct: %v", tile1)
	}

	var tile2 db.Tile
	if err = tileColl.FindOne(ctx, bson.D{{"mgrs_code", "18QZG"}}).Decode(&tile2); err != nil {
		if err == mongo.ErrNoDocuments {
			t.Error("could not find tile in database")
		}
		t.Fatal("failed getting tile")
	} else if (
		tile2.Date !=  primitive.NewDateTimeFromTime(time.Date(2020, time.Month(1), 29, 0, 0, 0, 0, location)) || 
		tile1.SourceSatellite != "S2A-L2A" || 
		len(tile1.Files) != 0) {
		t.Fatalf("tile1 not correct: %v", tile1)
	}

	// ensure the events were distributed
	eventColl := db.EventCollection(dbClient)
	if eventCount, err := eventColl.CountDocuments(ctx, bson.D{{}}); err != nil || eventCount != 4 {
		t.Errorf("expect 4 events but found %d", eventCount)
	}

	var events []db.Event
	cursor, err := eventColl.Find(ctx, bson.D{{"event_type", "RequestMapTask"}})
	if err = cursor.All(ctx, &events); err != nil {
		t.Fatal(err)
	}

	expectedFiles := map[string]bool{
		"sentinel-s2-l2a-cogs/39/P/UL/2019/9/S2A_39PUL_20190914_0_L2A/B04.tif": true,
		"sentinel-s2-l2a-cogs/39/P/UL/2019/9/S2A_39PUL_20190914_0_L2A/B08.tif": true,
		"sentinel-s2-l2a-cogs/39/P/UL/2019/9/S2A_39PUL_20190914_0_L2A/S2A_39PUL_20190914_0_L2A.json": true,
		"sentinel-s2-l2a-cogs/18/Q/ZG/2020/1/S2A_18QZG_20200129_0_L2A/B04.tif": true,
	}
	actualFiles := make(map[string]bool)
	for _, event := range events {
		actualFiles[event.Data["objectPath"]] = true
	}
	if !reflect.DeepEqual(expectedFiles, actualFiles) {
		t.Log(actualFiles)
		t.Fatal("file listing in events is not correct")
	}
}

func TestUTCFormattedDate(t *testing.T) {
	dateOptions := UTCFormattedDateOptions("")

	if len(dateOptions) != 2 {
		t.Fatalf("expected 2 options got %d", len(dateOptions))
	}

	t.Log(dateOptions)
	
}
