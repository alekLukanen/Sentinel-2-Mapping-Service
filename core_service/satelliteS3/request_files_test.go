package satelliteS3

import (
	"os"
	"path/filepath"
	"testing"
	"io/ioutil"
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
)


func TestGetSatelliteData(t *testing.T) {
	ctx := context.Background()
	objectSession, err := S3Session(ctx, SATELLITE_S3_INVENTORY_BUCKET)
	if err != nil {
		t.Fatal(err)
	}

	// upload example file
	file, err := os.Open("example_data/2022-12-17-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	objectKey := "example/invetory/inv1.json"
	uploader := manager.NewUploader(objectSession)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(SATELLITE_S3_INVENTORY_BUCKET), // Bucket to be used
		Key:    aws.String(objectKey),          // Name of the file to be saved
		Body:   file,                            // File
	})
	if err != nil {
		// Print the error and exit.
		t.Fatalf("Unable to upload: %v", err)
	}

	// ensure we can retrieve the file
	dir, err := os.MkdirTemp("", "test_object_store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	fileName := filepath.Join(dir, "test_file.json")
	err = GetObject(ctx, fileName, objectKey, SATELLITE_S3_INVENTORY_BUCKET)
	if err != nil {
		t.Fatal(err)
	}

	downloadedFileData, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}

	sourceFileData, err := ioutil.ReadFile("example_data/2022-12-17-manifest.json")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(downloadedFileData, sourceFileData) {
		t.Fatal("file data not equal")
	}
}