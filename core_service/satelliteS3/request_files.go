package satelliteS3

import (
	"os"
    "log"
	"fmt"
	"strings"
	"context"

	db "core_service/database"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
)


var (
	SATELLITE_S3_INVENTORY_BUCKET   = "default"
	SATELLITE_S3_IMAGE_BUCKET   = "default"
	SATELLITE_S3_INVENTORY_ENDPOINT = "http://localhost:9090"
	SATELLITE_S3_IMAGE_ENDPOINT = "http://localhost:9090"
)


func init() {

	if strings.HasSuffix(os.Args[0], ".test") {
		SATELLITE_S3_INVENTORY_BUCKET   = "default"
		SATELLITE_S3_IMAGE_BUCKET   = "default"
		SATELLITE_S3_INVENTORY_ENDPOINT = "http://localhost:9090"
		SATELLITE_S3_IMAGE_ENDPOINT = "http://localhost:9090"
	} else {
		SATELLITE_S3_INVENTORY_BUCKET   = "sentinel-cogs-inventory"
		SATELLITE_S3_IMAGE_BUCKET   = "sentinel-cogs"
		SATELLITE_S3_INVENTORY_ENDPOINT = "https://s3.us-west-2.amazonaws.com"
		SATELLITE_S3_IMAGE_ENDPOINT = db.GetEnvironmentVariableOrPanic("SATELLITE_S3_IMAGE_ENDPOINT")
	}

}

type UnknownBucketError struct{
	bucketName string
}
func (m *UnknownBucketError) Error() string {
	return fmt.Sprintf("Bucket named '%s' is not known", m.bucketName)
}

func S3Session(ctx context.Context, sourceBucket string) (*s3.Client, error) {
	if SATELLITE_S3_INVENTORY_ENDPOINT == "" || SATELLITE_S3_IMAGE_BUCKET == "" {
		log.Fatal("satelite s3 urls is not set")
	}

	var endpoint string
	if sourceBucket == SATELLITE_S3_INVENTORY_BUCKET {
		endpoint = SATELLITE_S3_INVENTORY_ENDPOINT
	} else if sourceBucket == SATELLITE_S3_IMAGE_BUCKET {
		endpoint = SATELLITE_S3_IMAGE_ENDPOINT
	} else {
		log.Fatal("unknown satellite s3 bucket")
	}

	// anonymous credentials
	s3Config, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithRegion("us-west-2"),
	)
	if err != nil {
		return nil, err
	}

	newSession := s3.NewFromConfig(s3Config, func (o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return newSession, nil
}

func GetObject(ctx context.Context, localPath, objectPath, sourceBucket string) error {
	log.Printf("GET %s %s\n", sourceBucket, objectPath)

	if sourceBucket != SATELLITE_S3_IMAGE_BUCKET && sourceBucket != SATELLITE_S3_INVENTORY_BUCKET {
		return &UnknownBucketError{bucketName: sourceBucket}
	}

	s3Session, err := S3Session(ctx, sourceBucket)
	if err != nil {
		return err
	}

	file, fileErr := os.Create(localPath)
	if fileErr != nil {
		return fileErr
	}
	defer file.Close()

	downloader := manager.NewDownloader(s3Session, func(d *manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 10MB per part
		d.Concurrency = 1 // number of goroutines downloading parts of the file
	})
	numBytes, err := downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(sourceBucket), 
		Key:    aws.String(objectPath),
	})
	if err != nil {
		log.Printf("Unable to download: %v\n", err)
		return err
	}

	log.Println("Downloaded", file.Name(), numBytes / 1_000_000, "MB")
	return nil
}

