package database

import (
	"context"
	"encoding/json"
	"time"
	"errors"

	geoTrans "core_service/geoTransformations"

	"github.com/golang/geo/s2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	geom "github.com/twpayne/go-geom"
)

type Geometry struct {
	Coordinates [][][]float64 `bson:"coordinates" json:"coordinates"`
	Type        string        `bson:"type" json:"type"`
}
func (obj *Geometry) ToBson() bson.D {
	return bson.D{
		{"type", obj.Type},
		{"coordinates", obj.Coordinates},
	}
}
func (obj *Geometry) ToJson() ([]byte, error) {
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}

type Boundary struct {
	ID       	primitive.ObjectID `bson:"_id" json:"id"`
	UserId      primitive.ObjectID `bson:"user_id" json:"userId"`
	Name     	string             `bson:"name" json:"name"`
	MgrsCodes 	[]string		   `bson:"mgrs_codes" json:"mgrsCodes"`
	Geometry 	Geometry           `bson:"geometry" json:"geometry"`
	Acres 		float64			   `bson:"acres" json:"acres"`
}

func BoundaryCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(DatabaseName()).Collection("boundary")
}

func SaveBoundary(ctx context.Context, client *mongo.Client, boundary *Boundary) error {
	coll := BoundaryCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()
	
	boundary.ID = primitive.NewObjectID()
	_, err := coll.InsertOne(mongoCtx, boundary)
	if err != nil {
		return err
	}
	return nil
}

func DeleteBoundary(ctx context.Context, dbClient *mongo.Client, filters bson.D) error {
	coll := BoundaryCollection(dbClient)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	// delete rasters for this boundary
	findBoundaryOpts := options.FindOne().SetProjection(bson.D{{"_id", 1}})
	var result bson.M
	if err := coll.FindOne(ctx, filters, findBoundaryOpts).Decode(&result); err != nil {
		return err
	}

	boundaryId := result["_id"].(primitive.ObjectID)
	if err := DeleteExistingBoundaryRastersByType(ctx, dbClient, boundaryId, ""); err != nil {
		return err
	}

	_, err := coll.DeleteOne(mongoCtx, filters)
	if err != nil {
		return err
	}
	return nil
}


func FindBoundaries(ctx context.Context, client *mongo.Client, filter bson.D, opts *options.FindOptions) (*[]Boundary, error) {
	coll := BoundaryCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	boundaries := make([]Boundary, 0, 100)
	cursor, err := coll.Find(mongoCtx, filter, opts)
	if err = cursor.All(mongoCtx, &boundaries); err != nil {
		return nil, err
	}

	return &boundaries, err
}

func FindBoundary(ctx context.Context, client *mongo.Client, filter bson.D) (*Boundary, error) {
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	coll := BoundaryCollection(client)
	opts := options.FindOne()

	var boundary Boundary
	err := coll.FindOne(mongoCtx, filter, opts).Decode(&boundary)
	
	if err != nil {
		return nil, err
	}
	return &boundary, nil
}

func UnmarshalJsonBoundary(data []byte) (*Boundary, error) {
	var boundaryData Boundary
	err := json.Unmarshal(data, &boundaryData)
	return &boundaryData, err
}

func MarshalJsonBoundary(boundary *Boundary) ([]byte, error) {
	jsonBytes, err := json.Marshal(boundary)
	return jsonBytes, err
}

func MarshalJsonBoundaries(boundaries *[]Boundary) ([]byte, error) {
	jsonBytes, err := json.Marshal(boundaries)
	return jsonBytes, err
}

func ComputeMgrsCodesFromGeometry(geometry *Geometry) []string {
	mgrsCodeIndex := make(map[string]bool)
	for _, boundaryGroup := range geometry.Coordinates {
		for _, point := range boundaryGroup {
			if mgrsCode, err := geoTrans.DefaultMGRSConverter.ConvertFromGeodetic(
				s2.LatLngFromDegrees(point[1], point[0]), 0,
			); err == nil {
				mgrsCodeIndex[mgrsCode[:5]] = true
			} else {
				continue
			}
		}
	}

	mgrsCodes := make([]string, 0, 3)
	for key, _ := range mgrsCodeIndex {
		mgrsCodes = append(mgrsCodes, key)
	}
	return mgrsCodes
}


func ComputeBoundaryArea(geometry *Geometry) (float64, error) {
	if geometry.Type != "Polygon" {
		return 0, errors.New("Geometry must be of type Polygon")
	} else if len(geometry.Coordinates) != 1 {
		return 0, errors.New("Geometry must have exactly one boundary")
	}

	var utmZone int
	utmCoordinates := make([]geom.Coord, 0, len(geometry.Coordinates[0]))
	for i, point := range geometry.Coordinates[0] {
		utmPoint, err := geoTrans.DefaultUTMConverter.ConvertFromGeodetic(
			s2.LatLngFromDegrees(point[1], point[0]), utmZone,
		)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			utmZone = utmPoint.Zone
		}
		utmCoordinates = append(utmCoordinates, geom.Coord{utmPoint.Easting, utmPoint.Northing})
	}

	points := make([][]geom.Coord, 1)
	points[0] = utmCoordinates

	polygon := geom.NewPolygon(geom.XY).MustSetCoords(points)

	area := polygon.Area()
	if area < 0 {
		polygon.Reverse()
		area = polygon.Area()
	}

	return area, nil
}