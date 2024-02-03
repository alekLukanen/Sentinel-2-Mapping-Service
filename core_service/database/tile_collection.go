package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TileFile struct {
	FileUse     string      `bson:"file_use" json:"fileUse"`
	Band 		string     	`bson:"band" json:"band"`
	Version 	int		    `bson:"version" json:"version"`
	Size 		int      	`bson:"size" json:"size"`
	ObjectPath 	string		`bson:"object_path" json:"objectPath"`
}
func (obj *TileFile) ToBson() bson.D {
	return bson.D{
		{"file_use", obj.FileUse},
		{"band", obj.Band},
		{"version", obj.Version},
		{"size", obj.Size},
		{"object_path", obj.ObjectPath},
	}
}

type Tile struct {
    ID 			 	primitive.ObjectID  `bson:"_id" json:"id"`
	UpdatedDate  	primitive.DateTime  `bson:"updated_date" json:"updatedDate"`
    Date         	primitive.DateTime  `bson:"date" json:"date"`
	MgrsCode     	string			    `bson:"mgrs_code" json:"mgrsCode"`
	SourceSatellite string		     	`bson:"source_satellite" json:"sourceSatellite"`
	Geometry 		Geometry			`bson:"geometry" json:"geometry"`
	Files       	[]TileFile          `bson:"files" json:"files"`
}
func (s *Tile) UniqueKey() string {
	date := s.Date.Time()
	return fmt.Sprintf("%d-%d-%d-%s-%s", date.Year(), date.Month(), date.Day(), s.MgrsCode, s.SourceSatellite)
}
func (obj *Tile) RefreshFromDb(ctx context.Context, client *mongo.Client) error {
	coll := TileCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	err := coll.FindOne(mongoCtx, bson.D{{"_id", obj.ID}}).Decode(obj)
	return err
}

func TileCollection(client *mongo.Client) *mongo.Collection{
	return client.Database(DatabaseName()).Collection("tile");
}


func FindTile(ctx context.Context, client *mongo.Client, filter bson.D) (*Tile, error) {
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	coll := TileCollection(client)
	opts := options.FindOne().SetSort(bson.D{{"date", -1}})

	var tile Tile
	err := coll.FindOne(mongoCtx, filter, opts).Decode(&tile)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &tile, nil
}


func FindTiles(ctx context.Context, client *mongo.Client, filter bson.D, opts *options.FindOptions) (*[]Tile, error) {
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	coll := TileCollection(client)

	tiles := make([]Tile, 0, 100)
	cursor, err := coll.Find(mongoCtx, filter, opts)
	if err = cursor.All(mongoCtx, &tiles); err != nil {
		return nil, err
	}

	return &tiles, err
}


func InsertFileIntoTile(ctx context.Context, client *mongo.Client, tile *Tile, tileFile *TileFile) error {
	coll := TileCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	filter := bson.D{
		{"_id", tile.ID},
		{"files.object_path", bson.D{{"$ne", tileFile.ObjectPath}}},
	}
	update := bson.D{
		{"$push", bson.D{
			{"files", tileFile.ToBson()},
		}},
	}

	err := coll.FindOneAndUpdate(
		mongoCtx,
		filter,
		update,
	).Err()
	return err
}


func UpdateTileAttribute(ctx context.Context, client *mongo.Client, tile *Tile, attribute string) error {
	coll := TileCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	var attributeData bson.D
	if attribute == "geometry" {
		attributeData = bson.D{
			{"updated_date", primitive.NewDateTimeFromTime(time.Now())},
			{"geometry", tile.Geometry.ToBson()},
		}
	} else {
		return errors.New("attribute not recognized")
	}

	filter := bson.D{
		{"_id", tile.ID},
	}
	update := bson.D{
		{"$set", attributeData},
	}

	err := coll.FindOneAndUpdate(
		mongoCtx,
		filter,
		update,
	).Err()
	return err
}


func UpdateOrCreateTile(ctx context.Context, client *mongo.Client, tile *Tile) (*Tile, error) {
	coll := TileCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()
	
	opts := options.FindOneAndUpdate().SetUpsert(true)
	filter := bson.D{
		{"date", tile.Date},
		{"mgrs_code", tile.MgrsCode},
		{"source_satellite", tile.SourceSatellite},
	}

	files := make([]bson.D, 0, 10)
	for _, file := range tile.Files {
		files = append(files, file.ToBson())
	}
	update := bson.D{
		{"$set", bson.D{
			{"updated_date", primitive.NewDateTimeFromTime(time.Now())},
			{"geometry", tile.Geometry.ToBson()},
			{"files", files},
		}},
	}
	var updatedTile Tile
	err := coll.FindOneAndUpdate(
		mongoCtx,
		filter,
		update,
		opts,
	).Decode(&updatedTile)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return tile, nil
		}
		return nil, err
	}
	return &updatedTile, nil
}