package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	db "core_service/database"
	satData "core_service/satelliteS3"

	"go.mongodb.org/mongo-driver/bson"
)

// to reduce bandwidth costs and improve application performance
// simply insert the map reference into the tiles Files array on the
// Tile. Then distribute a map event for each Boundary effected by
// the new map file.
func RequestMapTask(ctx context.Context, event *db.Event) error {
	log.Printf("RequestMapTask(%s)\n", event.ID.Hex())

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		return err
	}

	// construct the tile file object
	objectPath, hasObjectPathName := event.Data["objectPath"]
	if !hasObjectPathName || len(objectPath) == 0 {
		return errors.New("missing file name on event")
	}

	size, hasSize := event.Data["size"]
	if !hasSize || len(size) == 0 {
		return errors.New("missing size on event")
	}
	sizeValue, err := strconv.Atoi(size)
	if err != nil {
		return err
	}

	keyItems := strings.Split(objectPath, "/")
	if len(keyItems) != 8 {
		log.Println(keyItems)
		return errors.New("number of items not 7")
	}
	band := keyItems[7]

	fileUse := "satBand"
	jsonMetaDataFileName := fmt.Sprintf("%s.json", keyItems[6])
	if keyItems[7] == jsonMetaDataFileName {
		fileUse = "jsonMeta"
	}

	compoundItems := strings.Split(keyItems[6], "_")
	if len(compoundItems) != 5 {
		log.Println(compoundItems)
		return errors.New("compound items not equal to 4")
	}
	version, err := strconv.Atoi(compoundItems[3])
	if err != nil {
		return errors.New("version was not an integer")
	}

	tileFile := db.TileFile{
		FileUse:    fileUse,
		Band:       band,
		Version:    version,
		Size:       sizeValue,
		ObjectPath: objectPath,
	}
	tileInfo := parseTileFromObjectPath(objectPath)

	filter := bson.D{
		{"date", tileInfo.Date},
		{"mgrs_code", tileInfo.MgrsCode},
		{"source_satellite", tileInfo.SourceSatellite},
	}
	tile, err := db.FindTile(ctx, dbClient, filter)
	if err != nil {
		return err
	} else if tile == nil {
		log.Println("tile missing in database")
		return nil
	}

	// store the tile file object in the database by inserting
	// into the tile object.
	if err := db.InsertFileIntoTile(ctx, dbClient, tile, &tileFile); err != nil {
		log.Println("failed to insert tile file into a tile's file listing")
		return err
	}

	// if the file is a json meta file load the file and parse and save
	// the data geometry so it can be queried later on
	if fileUse == "jsonMeta" {
		if err := ParseDataGeometry(ctx, tile, objectPath); err != nil {
			log.Println("failed to parse geometry from file")
			return err
		}

		if err := db.UpdateTileAttribute(ctx, dbClient, tile, "geometry"); err != nil {
			log.Println("failed to update tile in database")
			return err
		}
	}

	// TODO : only publish if the tile has band 4 and band 8 and a boundary
	// publish an event to build the maps for the new tile if it doesn't
	// already exist
	existingBuildMapEventsCount, err := db.CountEvents(
		ctx,
		dbClient,
		bson.D{
			{"data.mgrsCode", tile.MgrsCode}, 
			{"started", false}, 
			{"passed", false}, 
			{"failed", false}, 
			{"event_type", "BuildBoundaryMapTask"},
		},
	)
	if err != nil {
		log.Println("failed to query existing events")
		return err
	} else if existingBuildMapEventsCount == 0 {
		log.Printf("distributing build map event for mgrs %s\n", tile.MgrsCode)
		buildMapEvent := db.Event{
			EventType:  "BuildBoundaryMapTask",
			Priority:   4,
			MaxAttemps: 1,
			Data: map[string]string{
				"mgrsCode": tile.MgrsCode,
			},
		}
		if err := db.SaveEvent(ctx, dbClient, &buildMapEvent); err != nil {
			log.Println("failed to publish build map event")
			return err
		}
	}

	return nil
}


type MetaData struct {
	Geometry 	db.Geometry 	`json:"geometry"`
}

func ParseDataGeometry(ctx context.Context, tile *db.Tile, objectPath string) error {
	if !strings.Contains(objectPath, ".json") {
		return errors.New("is not a json meta file")
	}

	dir, err := os.MkdirTemp(db.TEMP_DIR, "request_map_tasks")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir) // clean up

	jsonMeta := filepath.Join(dir, "meta.json")
	if err := satData.GetObject(ctx, jsonMeta, objectPath, satData.SATELLITE_S3_IMAGE_BUCKET); err != nil {
		log.Println("failed to request json meta from s3")
		return err
	}

	jsonMetaFile, err := os.Open(jsonMeta)
	if err != nil {
		return err
	}
	defer jsonMetaFile.Close()
	jsonMetaData, err := ioutil.ReadAll(jsonMetaFile)
	if err != nil {
		return err
	}

	var metaData MetaData
	if err := json.Unmarshal(jsonMetaData, &metaData); err != nil {
		return err
	}

	if metaData.Geometry.Type != "Polygon" || len(metaData.Geometry.Coordinates) < 1 || len(metaData.Geometry.Coordinates[0]) < 4 {
		return errors.New("geometry is not valid")
	}

	tile.Geometry = metaData.Geometry

	return nil
}