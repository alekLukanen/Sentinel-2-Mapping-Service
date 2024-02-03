package database

import (
	"context"
	"os"
	"strings"
	"time"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)



var mongodbUri string


func init() {

	if strings.HasSuffix(os.Args[0], ".test") {
		mongodbUri = "mongodb://root:pass@localhost:27017/"
	} else {
		mongodbUri = GetEnvironmentVariableOrPanic("MONGODB_URI")
	}

}


func DatabaseName() string {
	if len(os.Args) > 0 && strings.HasSuffix(os.Args[0], ".test") {
		return "test_db"
	} else {
		return "geo_spatial"
	}
}

func ConfigureDefaultDatabaseClient(ctx context.Context) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(mongodbUri).SetMaxPoolSize(20).SetWriteConcern(writeconcern.Majority())
	return DatabaseClient(ctx, clientOptions)
}


func DatabaseClient(ctx context.Context, options *options.ClientOptions) (*mongo.Client, error) {
	mongoCtx, _ := context.WithTimeout(ctx, 15*time.Second)
	client, err := mongo.Connect(mongoCtx, options)
	if err != nil {
		return nil, err
	}

	// Ping the primary
	if err := client.Ping(mongoCtx, readpref.Primary()); err != nil {
		return nil, err
	}
	return client, nil
}

func Disconnect(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
        log.Println(err)
    }
}

func DisconnectSafe(ctx context.Context, client *mongo.Client) error {
	if err := client.Disconnect(ctx); err != nil {
        return err
    }
	return nil
}
