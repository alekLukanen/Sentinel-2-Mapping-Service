package worker

import (
	"context"
	"fmt"
	"os"
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

func abs(value float32) float32 {
	if value < 0.0 {
		return -value
	}
	return value
}

func rastersAreEqual(expectedRaster, actualRaster db.Raster) bool {
	attributesEqual := (expectedRaster.BoundaryId == actualRaster.BoundaryId &&
		expectedRaster.ImagePath == actualRaster.ImagePath &&
		expectedRaster.Type == actualRaster.Type &&
		len(expectedRaster.MetaData.ImageBounds) == len(actualRaster.MetaData.ImageBounds) &&
		abs(expectedRaster.MetaData.RasterMax - actualRaster.MetaData.RasterMax) < 0.01 &&
		abs(expectedRaster.MetaData.RasterMin - actualRaster.MetaData.RasterMin) < 0.01 &&
		abs(expectedRaster.MetaData.RasterMedian - actualRaster.MetaData.RasterMedian) < 0.01 &&
		abs(expectedRaster.MetaData.RasterMean - actualRaster.MetaData.RasterMean) < 0.01)
	return attributesEqual
}


func TestBuildBoundaryMapTask(t *testing.T) {
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
	objectSession, err := satData.S3Session(ctx, satData.SATELLITE_S3_IMAGE_BUCKET)
	if err != nil {
		t.Fatal(err)
	}
	uploader := manager.NewUploader(objectSession)

	// create a user
	user := db.User{Name: "default_user_name"}
	if err := db.SaveUser(ctx, dbClient, &user); err != nil {
		t.Fatal(err)
	}

	// load the satellite data tiles into the database and s3
	tile := db.Tile{
		Date: primitive.NewDateTimeFromTime(time.Date(2022, time.Month(7), 16, 0, 0, 0, 0, location)),
		MgrsCode: "14TNR",
		SourceSatellite: "S2A",
		Geometry: db.Geometry{
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
		},
		Files: []db.TileFile{
			{
				FileUse: "satBand",
				Band: "B04.tif",
				Version: 0,
				Size: 99,
				ObjectPath: "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B04.tif",
			},
			{
				FileUse: "satBand",
				Band: "B08.tif",
				Version: 0,
				Size: 99,
				ObjectPath: "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B08.tif",
			},
		},
	}

	_, err = db.UpdateOrCreateTile(ctx, dbClient, &tile)
	if err != nil {
		t.Fatal(err)
	}

	// upload the band 4 file
	tileBand04File, err := os.Open("example_data/S2A_14TNR_20220716_0_L2A/B04.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer tileBand04File.Close()
	tileBand04Key := "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B04.tif"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_IMAGE_BUCKET),
		Key:    aws.String(tileBand04Key),
		Body:   tileBand04File,
	})
	if err != nil {
		t.Fatalf("Unable to upload: %v", err)
	}

	// upload the band 8 file
	tileBand08File, err := os.Open("example_data/S2A_14TNR_20220716_0_L2A/B08.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer tileBand08File.Close()
	tileBand08Key := "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B08.tif"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_IMAGE_BUCKET),
		Key:    aws.String(tileBand08Key),
		Body:   tileBand08File,
	})
	if err != nil {
		t.Fatalf("Unable to upload: %v", err)
	}

	// define some boundaries that exist in the tile
	boundary1 := db.Boundary{
		Name: "Boundary 1",
		UserId: user.ID,
		MgrsCodes: []string{"14TNR"},
		Geometry: db.Geometry{
			Type: "Polygon",
			Coordinates: [][][]float64{
				{
					{-98.29377108430018, 45.51545082233693},
					{-98.23596192672191, 45.513762969793305},
					{-98.23475756927279, 45.55341412123062},
					{-98.279318794906, 45.55004064352917},
					{-98.29377108430018, 45.51545082233693},
				},
			},
		},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary1); err != nil {
		t.Fatal("failed to save boundary")
	}

	boundary2 := db.Boundary{
		Name: "Boundary 2",
		UserId: user.ID,
		MgrsCodes: []string{"14TNR"},
		Geometry: db.Geometry{
			Type: "Polygon",
			Coordinates: [][][]float64{
				{
					{-98.17453969679522,
						45.74620810723795},
					{-98.12877411371237,
						45.708375215070674},
					{-98.12756975626274,
						45.74956979092943},
					{-98.17453969679522,
						45.74620810723795},
				},
			},
		},
	}
	if err := db.SaveBoundary(ctx, dbClient, &boundary2); err != nil {
		t.Fatal("failed to save boundary")
	}

	event := db.Event{
		EventType: "BuildBoundaryMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"mgrsCode": "14TNR",
		},
	}

	if err := BuildBoundaryMapTask(ctx, &event); err != nil {
		t.Fatal(err)
	}

	// validate that the boundaries have rasters
	// and that the images exist in the s3 instance
	filters := bson.D{{}}
	queryOpts := options.Find()
	rasters, err := db.FindRasters(ctx, dbClient, filters, queryOpts)
	if len(*rasters) != 2 {
		t.Fatalf("expected 2 rasters but found %d", len(*rasters))
	}

	rastersByBoundary := make(map[primitive.ObjectID]db.Raster)
	for _, raster := range *rasters {
		rastersByBoundary[raster.BoundaryId] = raster
	}

	raster1, exists := rastersByBoundary[boundary1.ID]
	if !exists {
		t.Fatal("boundary 1 missing raster")
	}
	expectedRaster1 := db.Raster{
		ID: raster1.ID,
		UserId: user.ID,
		BoundaryId: boundary1.ID,
		Type: db.TYPE_NDVI_MAP,
		ImagePath: fmt.Sprintf("%s%s", db.S3_IMAGE_PREFIX, raster1.ID.Hex()),
		MetaData: db.RasterMeta{
			ImageBounds: [][]float32{
				{45.51366193452552, -98.29379230898496}, 
				{45.553832217548006, -98.23475752841048},
			},
			RasterMax: 0.92525184,
			RasterMin: -1.0,
			RasterMedian: 0.64665991,
			RasterMean: 0.53262651,
		},
	}
	if !rastersAreEqual(expectedRaster1, raster1) {
		t.Log("raster for boundary 1 not created as expected")
		t.Fatal(raster1)
	}

	raster2, exists := rastersByBoundary[boundary2.ID]
	if !exists {
		t.Fatal("boundary 2 missing raster")
	}
	expectedRaster2 := db.Raster{
		ID: raster2.ID,
		UserId: user.ID,
		BoundaryId: boundary2.ID,
		Type: db.TYPE_NDVI_MAP,
		ImagePath: fmt.Sprintf("%s%s", db.S3_IMAGE_PREFIX, raster2.ID.Hex()),
		MetaData: db.RasterMeta{
			ImageBounds: [][]float32{
				{45.708335876464844, -98.17520904541016}, 
				{45.749977111816406, -98.12751007080078},
			},
			RasterMax: 0.881195068,
			RasterMin: -0.100977197,
			RasterMedian: 0.569156587,
			RasterMean: 0.520307064,
		},
	}
	if !rastersAreEqual(expectedRaster2, raster2) {
		t.Log("raster for boundary 2 not created as expected")
		t.Fatal(raster2)
	}

}


func TestPerformanceOfMapGeneration(t *testing.T) {
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
	objectSession, err := satData.S3Session(ctx, satData.SATELLITE_S3_IMAGE_BUCKET)
	if err != nil {
		t.Fatal(err)
	}
	uploader := manager.NewUploader(objectSession)

	// load the satellite data tiles into the database and s3
	tile := db.Tile{
		Date: primitive.NewDateTimeFromTime(time.Date(2022, time.Month(7), 16, 0, 0, 0, 0, location)),
		MgrsCode: "14TNR",
		SourceSatellite: "S2A",
		Geometry: db.Geometry{
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
		},
		Files: []db.TileFile{
			{
				FileUse: "satBand",
				Band: "B04.tif",
				Version: 0,
				Size: 99,
				ObjectPath: "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B04.tif",
			},
			{
				FileUse: "satBand",
				Band: "B08.tif",
				Version: 0,
				Size: 99,
				ObjectPath: "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B08.tif",
			},
		},
	}

	_, err = db.UpdateOrCreateTile(ctx, dbClient, &tile)
	if err != nil {
		t.Fatal(err)
	}

	// upload the band 4 file
	tileBand04File, err := os.Open("example_data/S2A_14TNR_20220716_0_L2A/B04.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer tileBand04File.Close()
	tileBand04Key := "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B04.tif"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_IMAGE_BUCKET),
		Key:    aws.String(tileBand04Key),
		Body:   tileBand04File,
	})
	if err != nil {
		t.Fatalf("Unable to upload: %v", err)
	}

	// upload the band 8 file
	tileBand08File, err := os.Open("example_data/S2A_14TNR_20220716_0_L2A/B08.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer tileBand08File.Close()
	tileBand08Key := "sentinel-s2-l2a-cogs/14/T/NR/2022/7/S2A_14TNR_20220716_0_L2A/B08.tif"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(satData.SATELLITE_S3_IMAGE_BUCKET),
		Key:    aws.String(tileBand08Key),
		Body:   tileBand08File,
	})
	if err != nil {
		t.Fatalf("Unable to upload: %v", err)
	}

	// define some boundaries that exist in the tile
	for i := 0; i < 25; i++ {
		boundary1 := db.Boundary{
			Name: fmt.Sprintf("Boundary %d", i),
			MgrsCodes: []string{"14TNR"},
			Geometry: db.Geometry{
				Type: "Polygon",
				Coordinates: [][][]float64{
					{
						{-98.29377108430018, 45.51545082233693},
						{-98.23596192672191, 45.513762969793305},
						{-98.23475756927279, 45.55341412123062},
						{-98.279318794906, 45.55004064352917},
						{-98.29377108430018, 45.51545082233693},
					},
				},
			},
		}
		if err := db.SaveBoundary(ctx, dbClient, &boundary1); err != nil {
			t.Fatal("failed to save boundary")
		}
	}

	event := db.Event{
		EventType: "BuildBoundaryMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"mgrsCode": "14TNR",
		},
	}

	if err := BuildBoundaryMapTask(ctx, &event); err != nil {
		t.Fatal(err)
	}

	// validate that the boundaries have rasters
	// and that the images exist in the s3 instance
	filters := bson.D{{}}
	queryOpts := options.Find()
	rasters, err := db.FindRasters(ctx, dbClient, filters, queryOpts)
	if len(*rasters) != 25 {
		t.Fatalf("expected 25 rasters but found %d", len(*rasters))
	}

}