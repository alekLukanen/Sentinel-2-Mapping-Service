package worker

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	db "core_service/database"
	satData "core_service/satelliteS3"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)


var (
	NDVI_SCRIPT 			string
	PROJECT_PYTHON_PATH 	string
)

func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		NDVI_SCRIPT = "./core_service/pyGeoSpatialApp/build_ndvi_map.py"
		PROJECT_PYTHON_PATH = "../.venv/bin/python"
	} else {
		NDVI_SCRIPT = db.GetEnvironmentVariableOrPanic("NDVI_SCRIPT")
		PROJECT_PYTHON_PATH = db.GetEnvironmentVariableOrPanic("PROJECT_PYTHON_PATH")
	}
}


func BuildBoundaryMapTask(ctx context.Context, event *db.Event) error {
	log.Printf("BuildBoundaryMapTask(%s)", event.ID.Hex())
	log.Println("event data:", event.Data)

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		return err
	}

	// find all boundaries effected by the new tile
	boundariesFilter := bson.D{
		{"mgrs_codes", event.Data["mgrsCode"]},
	}
	if boundaryId, exists := event.Data["boundaryId"]; exists {
		boundaryObjectId, err := primitive.ObjectIDFromHex(boundaryId)
		if err != nil {
			log.Println("malformed boundary id in event data")
			return err
		}
		boundariesFilter = append(boundariesFilter, bson.E{"_id", boundaryObjectId})
	}

	// get boundaries for each tile
	tileBoundaries, err := FindBoundariesForTile(ctx, dbClient, event.Data["mgrsCode"], boundariesFilter)
	if err != nil {
		log.Println("failed to get boundaries each for tiles")
		return err
	}

	for tileId, boundaries := range tileBoundaries {
		log.Println("tile id:", tileId)

		log.Println("number of boundaries effected by the tile:", len(*boundaries))
		if len(*boundaries) == 0 {
			log.Println("no boundaries were effected by the tile")
			continue
		}

		filter := bson.D{{"_id", tileId}}
		tile, err := db.FindTile(ctx, dbClient, filter)
		if err != nil {
			log.Println("failed to the tile by id")
			return err
		}

		if err := SetupAndBuildNDVIMaps(ctx, dbClient, boundaries, tile); err != nil {
			return err
		}

	}

	return nil
}


func FindBoundariesForTile(ctx context.Context, dbClient *mongo.Client, mgrsCode string, boundariesFilter bson.D) (map[primitive.ObjectID]*[]db.Boundary, error) {
	boundariesByTileId := make(map[primitive.ObjectID]*[]db.Boundary)

	// get the 5 most recent tiles
	filter := bson.D{{"mgrs_code", mgrsCode}}
	opts := options.Find()
	opts.SetSort(bson.D{{"date", -1}})
	opts.SetLimit(10)
	tiles, err := db.FindTiles(ctx, dbClient, filter, opts)
	if err != nil {
		log.Println("failed to get the most recent tiles")
		return nil, err
	}

	log.Println("number of tiles:", len(*tiles))
	if len(*tiles) == 0 {
		log.Println("no tiles were found")
		return boundariesByTileId, nil
	}

	for _, tile := range *tiles {
		filters := boundariesFilter
		filters = append(filters, bson.E{"geometry", bson.D{{"$geoIntersects", bson.D{{"$geometry", tile.Geometry}}}}})
		ops := options.Find()
		boundaries, err := db.FindBoundaries(ctx, dbClient, filters, ops)
		if err != nil {
			log.Println("had an error getting boundaries for tile")
			return nil, err
		}

		boundariesNotAlreadyClaimed := make([]db.Boundary, 0, len(*boundaries) / 2)
		for _, boundary := range *boundaries {

			boundaryClaimed := false
			for _, tileBoundaries := range boundariesByTileId {
				
				for _, tileBoundary := range *tileBoundaries {

					if tileBoundary.ID == boundary.ID {
						boundaryClaimed = true
					}

				}

			}

			if !boundaryClaimed {
				boundariesNotAlreadyClaimed = append(boundariesNotAlreadyClaimed, boundary)
			}

		}
		boundariesByTileId[tile.ID] = &boundariesNotAlreadyClaimed

	}

	return boundariesByTileId, nil
}


func SetupAndBuildNDVIMaps(ctx context.Context, dbClient *mongo.Client, boundaries *[]db.Boundary, tile *db.Tile) error {
	log.Println("SetupAndBuildNDVIMaps()")

	latestVersion := 0
	for _, file := range tile.Files {
		if file.Version > latestVersion {
			latestVersion = file.Version
		}
	}

	band04ObjectPath := ""
	band08ObjectPath := ""
	bandSCLObjectPath := ""
	for _, file := range tile.Files {
		if file.Version == latestVersion {
			if file.Band == "B04.tif" {
				band04ObjectPath = file.ObjectPath
			} else if file.Band == "B08.tif" {
				band08ObjectPath = file.ObjectPath
			} else if file.Band == "SCL.tif" {
				bandSCLObjectPath = file.ObjectPath
			}
		}
	}
	if band04ObjectPath == "" || band08ObjectPath == "" {
		return errors.New("tile did not have files for bands 4 and 8")
	}

	log.Println("band04ObjectPath:", band04ObjectPath)
	log.Println("band08ObjectPath:", band08ObjectPath)
	log.Println("bandSCLObjectPath:", bandSCLObjectPath)

	// download the tile data from satellite data s3
	dataDir, err := os.MkdirTemp(db.TEMP_DIR, "build_boundary_map_task")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)

	band04Path := filepath.Join(dataDir, "satData_band04.tif")
	band08Path := filepath.Join(dataDir, "satData_band08.tif")

	
	if err := satData.GetObject(ctx, band04Path, band04ObjectPath, satData.SATELLITE_S3_IMAGE_BUCKET); err != nil {
		log.Println("failed to get satellite data file band 04")
		return err
	}
	if err := satData.GetObject(ctx, band08Path, band08ObjectPath, satData.SATELLITE_S3_IMAGE_BUCKET); err != nil {
		log.Println("failed to get satellite data file band 08")
		return err
	}

	// optionally load the sceen classification layer
	if bandSCLObjectPath != "" {
		bandSCLPath := filepath.Join(dataDir, "satData_bandSCL.tif")

		if err := satData.GetObject(ctx, bandSCLPath, bandSCLObjectPath, satData.SATELLITE_S3_IMAGE_BUCKET); err != nil {
			log.Println("failed to get satellite data file band SCL")
			return err
		}

	}

	if err := BuildNDVIMaps(ctx, dbClient, boundaries, tile, dataDir); err != nil {
		log.Println("failed to build the ndvi maps")
		return err
	}

	return nil
}

func BuildNDVIMaps(ctx context.Context, dbClient *mongo.Client, boundaries *[]db.Boundary, tile *db.Tile, dataDir string) error {
	log.Println("BuildNDVIMaps()")

	boundaryPrefix := "boundary_geometry_"
	bandPrefix := "satData_band"
	rasterImagePrefix := "raster_image_"
	rasterMetaPrefix := "raster_meta_"

	if len(*boundaries) == 0 {
		log.Println("no boundaries were provided")
		return nil
	}

	// write the boundary data to the data directory
	WriteBoundaryFiles(boundaries, dataDir, boundaryPrefix)

	// call the python program to generate the rasters
	if err := CallPythonProgram(dataDir, bandPrefix, boundaryPrefix); err != nil {
		log.Println(err)
		return err
	}

	// read the png's and raster meta into a new raster for each boundary
	rasters, rasterImageFiles, err := BuildBoundaryRasters(boundaries, tile, dataDir, rasterImagePrefix, rasterMetaPrefix)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := SaveBoundaryRasters(ctx, dbClient, dataDir, rasters, rasterImageFiles); err != nil {
		log.Println(err)
		return err
	}

	return nil
}


func SaveBoundaryRasters(ctx context.Context, dbClient *mongo.Client, dataDir string, rasters *[]db.Raster, rasterImageFiles map[string]string) error {
	log.Println("SaveBoundaryRasters()")

	// iterate over each raster
	// storing the raster image to s3 before storing the object
	// to the database
	for _, raster := range *rasters {
		if rasterImagePath, exists := rasterImageFiles[raster.BoundaryId.Hex()]; exists {
			fullRasterImagePath := filepath.Join(dataDir, rasterImagePath)
			if err := raster.StoreRasterImage(ctx, fullRasterImagePath); err != nil {
				log.Println(err)
				continue
			}

			if err := db.DeleteExistingBoundaryRastersByType(ctx, dbClient, raster.BoundaryId, raster.Type); err != nil {
				log.Println(err)
				continue
			}

			if err := db.SaveRaster(ctx, dbClient, &raster); err != nil {
				log.Println(err)
				continue
			}
		}

	}

	return nil
}


func BuildBoundaryRasters(boundaries *[]db.Boundary, tile *db.Tile, dataDir, rasterImagePrefix, rasterMetaPrefix string) (*[]db.Raster, map[string]string, error) {
	log.Println("BuildBoundaryRasters()")

	rasterImageFiles := make(map[string]string)
	rasterMetaFiles := make(map[string]string)

	userIdByBoundaryId := make(map[primitive.ObjectID]primitive.ObjectID)
	for _, boundary := range(*boundaries) {
		userIdByBoundaryId[boundary.ID] = boundary.UserId
	}
	
	// create indexs for image and meta files
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, nil, err
	}
	for _, file := range files {
		fileName := file.Name()
		if (strings.HasPrefix(fileName, rasterImagePrefix) && 
			strings.HasSuffix(fileName, ".png")){
			boundaryId := strings.Replace(
				strings.Replace(fileName, rasterImagePrefix, "", 1), ".png", "", 1,
			)
			rasterImageFiles[boundaryId] = fileName
		} else if (strings.HasPrefix(fileName, rasterMetaPrefix) &&
			strings.HasSuffix(fileName, ".json")) {
			boundaryId := strings.Replace(
				strings.Replace(fileName, rasterMetaPrefix, "", 1), ".json", "", 1,
			)
			rasterMetaFiles[boundaryId] = fileName
		}
	}

	// build the raster objects
	rasters := make([]db.Raster, 0, len(rasterImageFiles))
	for boundaryId := range rasterImageFiles {
		log.Println(boundaryId)
		boundaryObjectId, err := primitive.ObjectIDFromHex(boundaryId)
		if err != nil {
			// the id must be malformed so skip this one
			log.Println("failed to read boundary id")
			continue
		}

		// create the raster object
		rasterMetaFile, exists := rasterMetaFiles[boundaryId]
		if !exists{
			continue
		}

		fullRasterMetaFilePath := filepath.Join(dataDir, rasterMetaFile)
		metaFile, err := os.Open(fullRasterMetaFilePath)
		if err != nil {
			log.Println(err)
			continue
		}
		metaData, err := ioutil.ReadAll(metaFile)
		if err != nil {
			log.Println(err)
			continue
		}
		metaFile.Close()

		rasterMeta, err := db.UnmarshalJsonRasterMeta(metaData)
		if err != nil {
			log.Println(err)
			continue
		}

		userId, exists := userIdByBoundaryId[boundaryObjectId]
		if !exists {
			log.Println("could not find user id by boundary id")
			continue
		}

		raster := db.Raster{
			BoundaryId: boundaryObjectId,
			UserId: userId,
			Type: db.TYPE_NDVI_MAP,
			ImagePath: "",
			MetaData: *rasterMeta,
			TileIds: []primitive.ObjectID{tile.ID,},
			TileDates: []primitive.DateTime{tile.Date},
		}
		rasters = append(rasters, raster)

	}

	return &rasters, rasterImageFiles, nil
}

func CallPythonProgram(dataDir, bandPrefix, boundaryPrefix string) error {
	log.Println("CallPythonProgram()")

	output, err := exec.Command(PROJECT_PYTHON_PATH, NDVI_SCRIPT, dataDir, bandPrefix, boundaryPrefix).Output()
	log.Println(string(output))
    if err != nil {
        return err
    }
	return nil
}


func WriteBoundaryFiles(boundaries *[]db.Boundary, dataDir string, boundaryPrefix string) {
	log.Println("WriteBoundaryFiles()")
	
	for _, boundary := range *boundaries {
		boundary_data, err := boundary.Geometry.ToJson()
		if err != nil {
			continue
		}

		boundary_file_path := filepath.Join(dataDir, fmt.Sprintf("%s%s.json", boundaryPrefix, boundary.ID.Hex()))
		boundary_file, err := os.Create(boundary_file_path)
		if err != nil {
			continue
		}
		boundary_file.Write(boundary_data)
		boundary_file.Close()
	}
}
