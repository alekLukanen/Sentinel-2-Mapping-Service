package database

import (
	"context"
	"log"
	"os"
	"time"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


var (
	ENVIRONMENT 		  string
	OBJECT_STORE_BUCKET   string
	OBJECT_STORE_KEY      string
	OBJECT_STORE_SECRET   string
	OBJECT_STORE_ENDPOINT string
	TEMP_DIR			  string
)



func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		ENVIRONMENT 		  = "local"
		OBJECT_STORE_BUCKET   = "default"
		OBJECT_STORE_KEY      = "key"
		OBJECT_STORE_SECRET   = "secret"
		OBJECT_STORE_ENDPOINT = "http://localhost:9090"
		TEMP_DIR 			  = ""
	} else {
		ENVIRONMENT = GetEnvironmentVariableOrPanic("ENVIRONMENT")
		OBJECT_STORE_BUCKET = GetEnvironmentVariableOrPanic("OBJECT_STORE_BUCKET")
		if ENVIRONMENT != "prod" {
			OBJECT_STORE_KEY = GetEnvironmentVariableOrPanic("OBJECT_STORE_KEY")
			OBJECT_STORE_SECRET = GetEnvironmentVariableOrPanic("OBJECT_STORE_SECRET")
			OBJECT_STORE_ENDPOINT = GetEnvironmentVariableOrPanic("OBJECT_STORE_ENDPOINT")
		}
		TEMP_DIR = "./appTemp"
	}
}


func ConfigureObjectStoreSession(ctx context.Context) (*s3.Client, error) {
	var newSession *s3.Client
	if ENVIRONMENT == "prod" {
		s3Config, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
		if err != nil {
			return nil, err
		}
		newSession = s3.NewFromConfig(s3Config)
	} else {
		creds := credentials.NewStaticCredentialsProvider( OBJECT_STORE_KEY, OBJECT_STORE_SECRET, "")
		s3Config, err := config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(creds), config.WithRegion("us-west-2"))
		if err != nil {
			return nil, err
		}
		newSession = s3.NewFromConfig(s3Config, func (o *s3.Options) {
			o.BaseEndpoint = aws.String(OBJECT_STORE_ENDPOINT)
			o.UsePathStyle = true
		})
	}
	return newSession, nil
}


func PutObject(ctx context.Context, localPath, objectPath string) error {
	objectSession, err := ObjectStoreSession(ctx)
	if err != nil {
		return err
	}

	file, err := os.Open(localPath)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	uploader := manager.NewUploader(objectSession)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(OBJECT_STORE_BUCKET),
		Key:    aws.String(objectPath),
		Body:   file,
	})
	if err != nil {
		// Print the error and exit.
		log.Printf("Unable to upload: %v", err)
		return err
	}

    dbClient, err := DefaultDatabaseClient(ctx)
	if err != nil {
		return err
	}

	object := Object{Path: objectPath, Exists: true}

	_, objectErr := UpdateOrCreateObject(ctx, dbClient, &object)
	if objectErr != nil {
		return objectErr
	}

	log.Println("Uploaded", objectPath)
	return nil
}

func GetObject(ctx context.Context, localPath, objectPath string) error {
	objectSession, err := ObjectStoreSession(ctx)
	if err != nil {
		return err
	}

	file, fileErr := os.Create(localPath)
	if fileErr != nil {
		return fileErr
	}
	defer file.Close()

	downloader := manager.NewDownloader(objectSession)
	numBytes, err := downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(OBJECT_STORE_BUCKET), 
		Key:    aws.String(objectPath),
	})
	if err != nil {
		log.Printf("Unable to download: %v\n", err)
		return err
	}

	log.Println("Downloaded", objectPath, numBytes, "bytes")
	return nil
}


func DeleteObject(ctx context.Context, objectPath string) error {
	objectSession, err := ObjectStoreSession(ctx)
	if err != nil {
		return err
	}

	objectIds := []types.ObjectIdentifier{types.ObjectIdentifier{Key: aws.String(objectPath)}}
	_, err = objectSession.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(OBJECT_STORE_BUCKET),
		Delete: &types.Delete{Objects: objectIds},
	})
	if err != nil {
		log.Printf("Unable to delete: %v\n", err)
		return err
	}

	dbClient, err := DefaultDatabaseClient(ctx)
	if err != nil {
		return err
	}

	object := Object{Path: objectPath, Exists: false}

	_, objectErr := UpdateOrCreateObject(ctx, dbClient, &object)
	if objectErr != nil {
		return objectErr
	}

	log.Println("Deleted", objectPath)
	return nil
}


type Object struct {
	ID     primitive.ObjectID `bson:"_id" json:"id"`
	Path   string             `bson:"path" json:"path"`
	Exists bool               `bson:"exists" json:"exists"`
}

func ObjectStoreCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(DatabaseName()).Collection("object_store")
}

func UpdateOrCreateObject(ctx context.Context, client *mongo.Client, object *Object) (*Object, error) {
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	coll := ObjectStoreCollection(client)
	opts := options.FindOneAndUpdate().SetUpsert(true)
	filter := bson.D{{"path", object.Path}}
	update := bson.D{{"$set", bson.D{{"exists", object.Exists}}}}
	var updatedObject Object
	err := coll.FindOneAndUpdate(
		mongoCtx,
		filter,
		update,
		opts,
	).Decode(&updatedObject)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return object, nil
		}
		return nil, err
	}
	return &updatedObject, err
}
