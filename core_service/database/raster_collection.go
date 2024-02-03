package database

import (
	"context"
	"time"
	"encoding/json"
	"path/filepath"
	"fmt"
	"io/ioutil"
	"encoding/base64"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


const (
	TYPE_NDVI_MAP = "NDVI_MAP"
	S3_IMAGE_PREFIX = "rasters/images/"
)


type RasterMeta struct {
	ImageBounds       				[][]float32  `bson:"image_bounds" json:"imageBounds"`
	RasterMin         				float32      `bson:"raster_min" json:"rasterMin"`
	RasterMax         				float32      `bson:"raster_max" json:"rasterMax"`
	RasterMedian      				float32      `bson:"raster_median" json:"rasterMedian"`
	RasterMean        				float32      `bson:"raster_mean" json:"rasterMean"`
	RasterPercentCoveredByClouds	float32		 `bson:"raster_percent_covered_by_clouds" json:"rasterPercentCoveredByClouds"`
}

func UnmarshalJsonRasterMeta(data []byte) (*RasterMeta, error) {
	var rasterMetaData RasterMeta
	err := json.Unmarshal(data, &rasterMetaData)
	return &rasterMetaData, err
}

func MarshalJsonRasterMeta(rasterMeta *RasterMeta) ([]byte, error) {
	jsonBytes, err := json.Marshal(rasterMeta)
	return jsonBytes, err
}


type Raster struct {
	ID         primitive.ObjectID  	`bson:"_id" json:"id"`
	UserId 	   primitive.ObjectID  	`bson:"user_id" json:"userId"`
	BoundaryId primitive.ObjectID  	`bson:"boundary_id" json:"boundaryId"`
	Type       string              	`bson:"type" json:"type"`
	ImagePath  string              	`bson:"image_path" json:"imagePath"`
	MetaData   RasterMeta          	`bson:"meta_data" json:"metaData"`
	TileIds    []primitive.ObjectID	`bson:"tile_ids" json:"tileIds"`
	TileDates  []primitive.DateTime `bons:"tile_dates" json:"tileDates"`
}
func (obj *Raster) StoreRasterImage(ctx context.Context, localPath string) error {
	if obj.ID == primitive.NilObjectID {
		obj.ID = primitive.NewObjectID()
	}
	obj.ImagePath = fmt.Sprintf("%s%s", S3_IMAGE_PREFIX, obj.ID.Hex())
	return PutObject(ctx, localPath, obj.ImagePath)
}
func (obj *Raster) RetrieveRasterImage(ctx context.Context, tmpDir string) (string, error) {
	filePath := filepath.Join(tmpDir, fmt.Sprintf("rasterImage_%s", obj.ID.Hex()))
	err := GetObject(ctx, filePath, obj.ImagePath)
	if err != nil {
		return "", err
	}

	imageData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	b64ImageData := base64.StdEncoding.EncodeToString(imageData)
	b64ImageData = fmt.Sprintf("data:image/png;base64,%s", b64ImageData)
	return b64ImageData, nil
}


func UnmarshalJsonRaster(data []byte) (*Raster, error) {
	var rasterData Raster
	err := bson.UnmarshalExtJSON(data, true, &rasterData)
	return &rasterData, err
}

func MarshalJsonRaster(raster *Raster) ([]byte, error) {
	jsonBytes, err := json.Marshal(raster)
	return jsonBytes, err
}

func RasterCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(DatabaseName()).Collection("raster")
}

func SaveRaster(ctx context.Context, client *mongo.Client, raster *Raster) error {
	coll := RasterCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	if raster.ID == primitive.NilObjectID {
		raster.ID = primitive.NewObjectID()
	}
	_, err := coll.InsertOne(mongoCtx, raster)
	if err != nil {
		return err
	}
	return nil
}

func FindRasters(ctx context.Context, client *mongo.Client, filter bson.D, opts *options.FindOptions) (*[]Raster, error) {
	coll := RasterCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	rasters := make([]Raster, 0, 100)
	cursor, err := coll.Find(mongoCtx, filter, opts)
	if err = cursor.All(mongoCtx, &rasters); err != nil {
		return nil, err
	}

	return &rasters, err
}

func FindRaster(ctx context.Context, client *mongo.Client, filter bson.D) (*Raster, error) {
	coll := RasterCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	var raster Raster
	opts := options.FindOne()
	err := coll.FindOne(mongoCtx, filter, opts).Decode(&raster)
	if err != nil {
		return nil, err
	}

	return &raster, nil
}

func DeleteRaster(ctx context.Context, client *mongo.Client, filters bson.D) error {
	coll := RasterCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	// find the raster images to be deleted
	findOpts := options.FindOne().SetProjection(bson.D{{"_id", 0}, {"image_path", 1}})
	var result bson.M
	if err := coll.FindOne(ctx, filters, findOpts).Decode(&result); err != nil {
		return err
	}
	
	// delete the objects from s3
	objectPath := result["image_path"].(string)
	if err := DeleteObject(ctx, objectPath); err != nil {
		return err
	}

	_, err := coll.DeleteOne(mongoCtx, filters)
	if err != nil {
		return err
	}
	return nil
}


func DeleteExistingBoundaryRastersByType(ctx context.Context, dbClient *mongo.Client, boundaryId primitive.ObjectID, rasterType string) error {
	filters := bson.D{{"boundary_id", boundaryId}}
	if rasterType != "" {
		filters = append(filters, bson.E{"type", rasterType})
	}
	opts := options.Find()
	rasters, err := FindRasters(ctx, dbClient, filters, opts)
	if err != nil {
		return err
	}

	for _, raster := range *rasters {
		rasterFilter := bson.D{{"_id", raster.ID}}
		err = DeleteRaster(ctx, dbClient, rasterFilter)
		if err != nil {
			return err
		}
	}

	return nil
}
