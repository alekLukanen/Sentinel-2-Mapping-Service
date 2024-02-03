package worker

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alekLukanen/csv-line-filter"

	db "core_service/database"
	satData "core_service/satelliteS3"
)

type ManifestFileItem struct {
	Key string `json:"key"`
}

type ManifestData struct {
	Files []ManifestFileItem `json:"files"`
}

func UTCFormattedDateOptions(date string) []string {
	var formattedDate string
	if date == "" {
		currentUTCDate := time.Now().UTC()
		yesterday := currentUTCDate.AddDate(0, 0, -1)

		// this just formats the date into a string 
		// with the given format example
		formattedDate = yesterday.Format("2006-01-02")
	} else {
		formattedDate = date
	}
	return []string{
		fmt.Sprintf("%sT00-00Z", formattedDate),
		fmt.Sprintf("%sT01-00Z", formattedDate),
	}
}

func RequestCurrentIndexFilesTask(ctx context.Context, event *db.Event) error {
	log.Printf("RequestCurrentIndexFilesTask(%s)\n", event.ID.Hex())

	// request the manifest.json file from the inventory
	// ensure we can retrieve the file
	dir, err := os.MkdirTemp(db.TEMP_DIR, "process_current_index_file")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir) // clean up

	manifestDate := event.Data["manifestDate"]
	log.Println("manifestDate: ", manifestDate)
	dateKeyOptions := UTCFormattedDateOptions(manifestDate)

	manifestFileName := filepath.Join(dir, "manifest_file.json")
	foundFile := false
	for _, dateKey := range dateKeyOptions {
		objectPath := fmt.Sprintf("sentinel-cogs/sentinel-cogs/%s/manifest.json", dateKey)
		err = satData.GetObject(ctx, manifestFileName, objectPath, satData.SATELLITE_S3_INVENTORY_BUCKET)
		if err != nil {
			log.Println(err)
			continue
		} else {
			foundFile = true
			log.Println("found manifest file: " + objectPath)
			break
		}
	}

	if !foundFile {
		log.Println("could not find index file")
		return err
	}

	// from the manifest json file get the first gzipped csv file with
	// all current s3 files. Use this to request the tiled satellite data
	manifestFile, err := ioutil.ReadFile(manifestFileName)
	if err != nil {
		return err
	}

	var manifestJsonData ManifestData
	if err := json.Unmarshal(manifestFile, &manifestJsonData); err != nil {
		return err
	}

	log.Println("index file count in manifest.json: ", len(manifestJsonData.Files))
	if len(manifestJsonData.Files) == 0 {
		log.Println("manifest.json file doesn't contain any csv index files")
		return err
	}

	// loop over each index file in the manifest.json
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		return err
	}

	settingColl := db.SettingCollection(dbClient)
	filters := bson.D{{}}
	var setting db.Setting
	err = settingColl.FindOne(ctx, filters).Decode(&setting)
	if err != nil {
		log.Println("failed to load settings")
		return err
	}
	log.Println("setting object used: ", setting)


	compressedCsvIndexFileName := filepath.Join(dir, "index.csv.gz")
	for fileIndex, fileItem := range manifestJsonData.Files {
		csvIndexFileKey := fileItem.Key

		log.Printf("[%d / %d]requesting csv index file: %s\n", fileIndex+1, len(manifestJsonData.Files), csvIndexFileKey)
		
		if err := satData.GetObject(ctx, compressedCsvIndexFileName, csvIndexFileKey, satData.SATELLITE_S3_INVENTORY_BUCKET); err != nil {
			log.Println("failed to request csv index file from s3")
			return err
		}

		// parse the inventory file and produce tile objects in the database
		// these will be requested by another task set
		tiles, err := CreateTilesFromCsvIndexFile(compressedCsvIndexFileName, &setting)
		if err != nil {
			log.Println("failed to create tiles")
			return err
		}

		if err := UpdateOrCreateTiles(ctx, tiles, dbClient); err != nil {
			log.Println("failed to save tiles to db")
			return err
		}

		// publish events to process the new tile files
		events, err := CreateTileFileEventsFromCsvIndexFile(compressedCsvIndexFileName, &setting)
		if err != nil {
			log.Println("failed to create tile file events")
			return err
		}

		if err := PublishTileFileEvents(ctx, events, dbClient); err != nil {
			log.Println("faild to save tiles file events to db")
			return err
		}
		
	}
	return nil
}

func PublishTileFileEvents(ctx context.Context, events *[]db.Event, dbClient *mongo.Client) error {
	log.Printf("creating %d events", len(*events))

	for _, event := range *events {
		if err := db.SaveEvent(ctx, dbClient, &event); err != nil {
			return err
		}
	}

	return nil
}

func CreateTileFileEventsFromCsvIndexFile(compressedCsvIndexFileName string, setting *db.Setting) (*[]db.Event, error) {
	compressedCsvFile, err := os.Open(compressedCsvIndexFileName)
	if err != nil {
		return nil, err
	}
	defer compressedCsvFile.Close()

	compressedCsvIndexFileReader, err := gzip.NewReader(compressedCsvFile)
	if err != nil {
		return nil, err
	}
	defer compressedCsvIndexFileReader.Close()

	lineFilterExpression := lineFilterRegularExpression(setting)
	csvLineFilterReader, err := csvLineFilter.NewCSVLineFilter(compressedCsvIndexFileReader, lineFilterExpression)
    if err != nil {
        return nil, err
    }

	utmZones := make(map[string]bool)
	for _, zone := range setting.UtmZones {
		utmZones[zone] = true
	}
	tileFiles := make(map[string]bool)
	for _, fileType := range setting.TileFiles {
		tileFiles[fileType] = true
	}
	startDate := setting.TileStartDate.Time()

	csvReader := csv.NewReader(csvLineFilterReader)
	events := make([]db.Event, 0, 1000)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if !recordIsConsumeable(record, utmZones, tileFiles, startDate) {
			continue
		}

		event := parseEventFromRecord(record)
		events = append(events, event)
	}

	return &events, nil
}

func parseEventFromRecord(record []string) db.Event {
	return db.Event{
		EventType: "RequestMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"objectPath": record[1],
			"size": record[2],
		},
	}
}

func UpdateOrCreateTiles(ctx context.Context, tiles *[]db.Tile, dbClient *mongo.Client) error {
	log.Printf("creating %d tiles", len(*tiles))

	for _, tile := range *tiles {
		if _, err := db.UpdateOrCreateTile(ctx, dbClient, &tile); err != nil {
			return err
		}
	}

	return nil
}

func CreateTilesFromCsvIndexFile(compressedCsvIndexFileName string, setting *db.Setting) (*[]db.Tile, error) {
	compressedCsvFile, err := os.Open(compressedCsvIndexFileName)
	if err != nil {
		return nil, err
	}
	defer compressedCsvFile.Close()

	compressedCsvIndexFileReader, err := gzip.NewReader(compressedCsvFile)
	if err != nil {
		return nil, err
	}
	defer compressedCsvIndexFileReader.Close()

	lineFilterExpression := lineFilterRegularExpression(setting)
	csvLineFilterReader, err := csvLineFilter.NewCSVLineFilter(compressedCsvIndexFileReader, lineFilterExpression)
    if err != nil {
        return nil, err
    }

	utmZones := make(map[string]bool)
	for _, zone := range setting.UtmZones {
		utmZones[zone] = true
	}
	tileFiles := make(map[string]bool)
	for _, fileType := range setting.TileFiles {
		tileFiles[fileType] = true
	}
	startDate := setting.TileStartDate.Time()

	csvReader := csv.NewReader(csvLineFilterReader)
	tileMap := make(map[string]db.Tile)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if !recordIsConsumeable(record, utmZones, tileFiles, startDate) {
			continue
		}

		tile := parseTileFromRecord(record)
		if _, exists := tileMap[tile.UniqueKey()]; exists == false {
			tileMap[tile.UniqueKey()] = tile
		}
	}

	tiles := make([]db.Tile, 0, len(tileMap))
	for _, value := range tileMap {
		tiles = append(tiles, value)
	}
	return &tiles, nil
}

func recordIsConsumeable(record []string, validUtmZones, validFiles map[string]bool, startDate time.Time) bool {

	// prefilt the records since only a few zones will be used
	containsValidUtmZone := false
	for validUtmZone, _ := range validUtmZones {
		if strings.Contains(record[1], validUtmZone) {
			containsValidUtmZone = true
		}
	}
	if !containsValidUtmZone {
		return false
	}

	if len(record) != 4 {
		return false
	}

	keyItems := strings.Split(record[1], "/")
	if len(keyItems) != 8 {
		return false
	}
	utmCode := fmt.Sprintf("%s%s", keyItems[1], keyItems[2])
	if _, exists := validUtmZones[utmCode]; exists != true {
		return false
	}

	jsonMetaData := fmt.Sprintf("%s.json", keyItems[6])
	if _, exists := validFiles[keyItems[7]]; exists != true && keyItems[7] != jsonMetaData {
		return false
	}

	compoundItems := strings.Split(keyItems[6], "_")
	if len(compoundItems) != 5 {
		return false
	}
	sourceSatellite := fmt.Sprintf("%s-%s", compoundItems[0], compoundItems[4])
	if sourceSatellite != "S2A-L2A" && sourceSatellite != "S2B-L2A" {
		return false
	}

	dateItem := compoundItems[2]
	year, err := strconv.Atoi(dateItem[:4])
    if err != nil {
        return false
    }
	month, err := strconv.Atoi(dateItem[4:6])
    if err != nil {
        return false
    }
	day, err := strconv.Atoi(dateItem[6:])
    if err != nil {
        return false
    }

	location, err := time.LoadLocation("UTC")
	if err != nil {
		return false
	}
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, location)
	if !date.After(startDate) {
		return false
	}

	return true
}

func parseTileFromRecord(record []string) db.Tile {
	return parseTileFromObjectPath(record[1])
}
func parseTileFromObjectPath(objectPath string) db.Tile {
	keyItems := strings.Split(objectPath, "/")
	mgrsCode := fmt.Sprintf("%s%s%s", keyItems[1], keyItems[2], keyItems[3])

	compoundItems := strings.Split(keyItems[6], "_")
	dateItem := compoundItems[2]
	year, _ := strconv.Atoi(dateItem[:4])
	month, _ := strconv.Atoi(dateItem[4:6])
	day, _ := strconv.Atoi(dateItem[6:])
	location, _ := time.LoadLocation("UTC")
	date := primitive.NewDateTimeFromTime(time.Date(year, time.Month(month), day, 0, 0, 0, 0, location))
	sourceSatellite := fmt.Sprintf("%s-%s", compoundItems[0], compoundItems[4])

	tile := db.Tile{
		Date: date,
		MgrsCode: mgrsCode,
		SourceSatellite: sourceSatellite,
	}
	return tile
}

func lineFilterRegularExpression(setting *db.Setting) string {

	if len(setting.UtmZones) == 0 {
		return ""
	}

	regularExpression := fmt.Sprintf("(_%s)", setting.UtmZones[0])
	for _, zone := range setting.UtmZones[1:] {
		regularExpression = fmt.Sprintf("%s|(_%s)", regularExpression, zone)
	}

	return regularExpression
}