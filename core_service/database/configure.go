package database

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ERROR_DATABASE_NOT_CONFIGURED             = errors.New("Database not configured")
	ERROR_OBJECT_STORE_SESSION_NOT_CONFIGURED = errors.New("Object store not configured")
)

// database session for package
var databaseClient *mongo.Client

func DefaultDatabaseClient(ctx context.Context) (*mongo.Client, error) {
	if databaseClient == nil {
		return nil, ERROR_DATABASE_NOT_CONFIGURED
	}
	return databaseClient, nil
}

// object store session for package
var objectStoreSession *s3.Client

func ObjectStoreSession(ctx context.Context) (*s3.Client, error) {
	if objectStoreSession == nil {
		return nil, ERROR_OBJECT_STORE_SESSION_NOT_CONFIGURED
	}
	return objectStoreSession, nil
}

func Configure(ctx context.Context) error {
	log.Println("- configuring the database connection")
	newClient, err := ConfigureDefaultDatabaseClient(ctx)
	if err != nil {
		log.Println("failed to start database client")
		return err
	}
	databaseClient = newClient

	log.Println("- configuring the s3/object store client")
	newObjectStoreClient, err := ConfigureObjectStoreSession(ctx)
	if err != nil {
		log.Println("failed to create the s3 client")
		return err
	}
	objectStoreSession = newObjectStoreClient

	log.Println("--- database configuration complete ---")

	return nil
}
