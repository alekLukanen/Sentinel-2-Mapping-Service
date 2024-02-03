package database

import (
	"context"
	"time"
	
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)


type Setting struct {
    ID 			   primitive.ObjectID  `bson:"_id" json:"id"`
	UtmZones   	   []string            `bson:"utm_zones" json:"utmZones"`
	TileFiles	   []string			   `bson:"tile_files" json:"tileFiles"`
    TileStartDate  primitive.DateTime  `bson:"tile_start_date" json:"tileStartDate"`
}


func SettingCollection(client *mongo.Client) *mongo.Collection{
	return client.Database(DatabaseName()).Collection("setting");
}

func SaveSetting(ctx context.Context, client *mongo.Client, setting *Setting) error {
	coll := SettingCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()
	
	setting.ID = primitive.NewObjectID()
	_, err := coll.InsertOne(mongoCtx, setting)
	if err != nil {
		return err
	}
	return nil
}