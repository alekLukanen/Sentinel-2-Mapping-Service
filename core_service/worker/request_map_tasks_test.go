package worker

import (
	"context"
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
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestRequestMapTask(t *testing.T) {
	db.CleanTestDatabase()

	location, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// create a boundary for testing the map events
	boundary1 := db.Boundary{
		MgrsCodes: []string{"18QZG"},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary1); err != nil {
		t.Fatal(err)
	}

	boundary2 := db.Boundary{
		MgrsCodes: []string{"19AAA", "20BBB"},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary2); err != nil {
		t.Fatal(err)
	}

	// 18QZK_20200526
	tile := db.Tile{
		Date: primitive.NewDateTimeFromTime(time.Date(2020, time.Month(1), 29, 0, 0, 0, 0, location)),
		MgrsCode: "18QZG",
		SourceSatellite: "S2A-L2A",
	}
	_, err = db.UpdateOrCreateTile(ctx, dbClient, &tile)
	if err != nil {
		t.Fatal(err)
	}
	updatedTile, err := db.FindTile(ctx, dbClient, bson.D{{}})
	if err != nil {
		t.Fatal(err)
	}

	event1 := db.Event{
		EventType: "RequestMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"objectPath": "sentinel-s2-l2a-cogs/18/Q/ZG/2020/1/S2A_18QZG_20200129_0_L2A/B04.tif",
			"size": "99",
		},
	}

	if err = RequestMapTask(ctx, &event1); err != nil {
		t.Fatal(err)
	}

	if err = updatedTile.RefreshFromDb(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if len(updatedTile.Files) != 1 {
		t.Fatal("should have exactly one file")
	}

	expectedTileFile := db.TileFile{
		FileUse: "satBand",
		Band: "B04.tif",
		Version: 0,
		Size: 99,
		ObjectPath: "sentinel-s2-l2a-cogs/18/Q/ZG/2020/1/S2A_18QZG_20200129_0_L2A/B04.tif",
	}
	if expectedTileFile != updatedTile.Files[0] {
		t.Fatal("tile files not equal")
	}

	expectedBuildMapEvent := db.Event{
		EventType:  "BuildBoundaryMapTask",
		Priority:   4,
		MaxAttemps: 1,
		Data: map[string]string{
			"mgrsCode": "18QZG",
		},
	}
	buildMapEvents, err := db.FindEvents(
		ctx,
		dbClient,
		bson.D{{"event_type", "BuildBoundaryMapTask"}},
		options.Find(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(*buildMapEvents) != 1 {
		t.Fatal("should have 1 build map event")
	}

	buildMapEvent := (*buildMapEvents)[0]
	if (
		buildMapEvent.Priority != expectedBuildMapEvent.Priority || 
		buildMapEvent.MaxAttemps != expectedBuildMapEvent.MaxAttemps || 
		buildMapEvent.Data["mgrsCode"] != expectedBuildMapEvent.Data["mgrsCode"]) {
		t.Fatal("build map event not correct")
	}

	// add another file
	event2 := db.Event{
		EventType: "RequestMapTask",
		MaxAttemps: 1,
		Priority: 4,
		Data: map[string]string{
			"objectPath": "sentinel-s2-l2a-cogs/18/Q/ZG/2020/1/S2A_18QZG_20200129_0_L2A/B08.tif",
			"size": "99",
		},
	}

	if err = RequestMapTask(ctx, &event2); err != nil {
		t.Fatal(err)
	}

	if err = updatedTile.RefreshFromDb(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if len(updatedTile.Files) != 2 {
		t.Fatal("should have exactly one file")
	}

	expectedTileFile = db.TileFile{
		FileUse: "satBand",
		Band: "B08.tif",
		Version: 0,
		Size: 99,
		ObjectPath: "sentinel-s2-l2a-cogs/18/Q/ZG/2020/1/S2A_18QZG_20200129_0_L2A/B08.tif",
	}
	if expectedTileFile != updatedTile.Files[1] {
		t.Fatal("tile files not equal")
	}

	buildMapEvents, err = db.FindEvents(
		ctx,
		dbClient,
		bson.D{{"event_type", "BuildBoundaryMapTask"}},
		options.Find(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(*buildMapEvents) != 1 {
		t.Fatalf("expected 1 build map event but found %d", len(*buildMapEvents))
	}

	secondTryBuildMapEvent := (*buildMapEvents)[0]
	if (
		secondTryBuildMapEvent.Priority != expectedBuildMapEvent.Priority || 
		secondTryBuildMapEvent.MaxAttemps != expectedBuildMapEvent.MaxAttemps || 
		secondTryBuildMapEvent.Data["mgrsCode"] != expectedBuildMapEvent.Data["mgrsCode"] ||
		secondTryBuildMapEvent.UpdatedDate != buildMapEvent.UpdatedDate) {
		t.Fatal("build map event not correct")
	}
}


func TestRequestMapTaskWithJsonFile(t *testing.T) {
	// tests that a json meta file can be inserted, requested and parsed
	// the geometry should be stored on the tile itself

	db.CleanTestDatabase()

	ctx := context.Background()

	objectSession, err := satData.S3Session(ctx, satData.SATELLITE_S3_IMAGE_BUCKET)
	if err != nil {
		t.Fatal(err)
	}
	uploader := manager.NewUploader(objectSession)

	
	// upload the json meta file to the satS3
	tileJsonMetaFile, err := os.Open("example_data/S2A_14TNR_20220716_0_L2A/S2A_14TNR_20220716_0_L2A.json")
	if err != nil {
		t.Fatal(err)
	}
	defer tileJsonMetaFile.Close()

	jsonMetaKey := "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/S2A_14TNR_20220716_0_L2A.json"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_IMAGE_BUCKET),
		Key:    aws.String(jsonMetaKey),
		Body:   tileJsonMetaFile,
	})
	if err != nil {
		t.Fatalf("Unable to upload: %v", err)
	}

	location, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// create a boundary for testing the map events
	boundary1 := db.Boundary{
		MgrsCodes: []string{"14TNR"},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary1); err != nil {
		t.Fatal(err)
	}

	boundary2 := db.Boundary{
		MgrsCodes: []string{"19AAA", "20BBB"},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary2); err != nil {
		t.Fatal(err)
	}

	tile := db.Tile{
		Date: primitive.NewDateTimeFromTime(time.Date(2022, time.Month(7), 16, 0, 0, 0, 0, location)),
		MgrsCode: "14TNR",
		SourceSatellite: "S2A-L2A",
	}
	_, err = db.UpdateOrCreateTile(ctx, dbClient, &tile)
	if err != nil {
		t.Fatal(err)
	}
	updatedTile, err := db.FindTile(ctx, dbClient, bson.D{{}})
	if err != nil {
		t.Fatal(err)
	}

	event1 := db.Event{
		EventType: "RequestMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"objectPath": "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/S2A_14TNR_20220716_0_L2A.json",
			"size": "15000",
		},
	}

	if err = RequestMapTask(ctx, &event1); err != nil {
		t.Fatal(err)
	}

	if err = updatedTile.RefreshFromDb(ctx, dbClient); err != nil {
		t.Fatal(err)
	}

	if len(updatedTile.Files) != 1 {
		t.Error(updatedTile.Files)
		t.Fatal("should have exactly one file")
	}

	expectedTileFile := db.TileFile{
		FileUse: "jsonMeta",
		Band: "S2A_14TNR_20220716_0_L2A.json",
		Version: 0,
		Size: 15000,
		ObjectPath: "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/S2A_14TNR_20220716_0_L2A.json",
	}
	if expectedTileFile != updatedTile.Files[0] {
		t.Error(updatedTile.Files)
		t.Fatal("tile files not equal")
	}

	// validate that the geometry on the tile is correct
	expectedGeometry := db.Geometry{
		Type: "Polygon",
		Coordinates: [][][]float64{
			{
				{-98.27917493756021, 46.05129235113481},
				{-97.58110900701091, 46.04475738929435},
				{-97.60575860695822, 45.056757746793146},
				{-98.61863006283437, 45.06463224445008},
				{-98.27917493756021, 46.05129235113481},
			},
		},
	}
	if expectedGeometry.Type != updatedTile.Geometry.Type || !reflect.DeepEqual(expectedGeometry.Coordinates, updatedTile.Geometry.Coordinates) {
		t.Error(updatedTile.Geometry)
		t.Fatal("tile geometry not equal")
	}

	expectedBuildMapEvent := db.Event{
		EventType:  "BuildBoundaryMapTask",
		Priority:   4,
		MaxAttemps: 1,
		Data: map[string]string{
			"mgrsCode": "14TNR",
		},
	}
	buildMapEvents, err := db.FindEvents(
		ctx,
		dbClient,
		bson.D{{"event_type", "BuildBoundaryMapTask"}},
		options.Find(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(*buildMapEvents) != 1 {
		t.Fatal("should have 1 build map event")
	}

	buildMapEvent := (*buildMapEvents)[0]
	if (
		buildMapEvent.Priority != expectedBuildMapEvent.Priority || 
		buildMapEvent.MaxAttemps != expectedBuildMapEvent.MaxAttemps || 
		buildMapEvent.Data["mgrsCode"] != expectedBuildMapEvent.Data["mgrsCode"]) {
		t.Fatal("build map event not correct")
	}

}