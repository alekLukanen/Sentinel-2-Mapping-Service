package database

import (
	"os"
	"io/ioutil"
	"path/filepath"
	"bytes"
	"testing"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestObjectStoreUploadAndDownload(t *testing.T) {
	CleanTestDatabase()

	ctx := context.Background()

	dir, err := os.MkdirTemp("", "test_object_store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	fileName := filepath.Join(dir, "test_file.json")

	objectKey := "data/file.json"
	err = PutObject(ctx, "example_data/boundary_shape1.json", objectKey)
	if err != nil {
		t.Fatal(err)
	}
	
	err = GetObject(ctx, fileName, objectKey)
	if err != nil {
		t.Fatal(err)
	}

	downloadedFileData, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}

	sourceFileData, err := ioutil.ReadFile("example_data/boundary_shape1.json")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(downloadedFileData, sourceFileData) {
		t.Fatal("file data not equal")
	}

	// check if the object was stored in the database
	dbClient, err := DefaultDatabaseClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	objColl := ObjectStoreCollection(dbClient)

	var result Object
	err = objColl.FindOne(ctx, bson.D{{"path", objectKey}}).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			t.Error("could not find object path in database")
		}
		t.Fatal("failed getting object")
	}
}