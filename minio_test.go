package main

import (
	"github.com/minio/minio-go"
	"log"
	"os"
	"testing"
	//"strings"
)

func upload() {

	endpoint := "minio.uvadcos.io"
	accessKey := "minioadmin"
	secretKey := "miniosecret"

	// initialize client object
	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		//
		log.Fatalln(err)
	}

	bucketName := "ithriv"

	err = minioClient.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(bucketName)
		if errBucketExists == nil && exists {
			log.Printf("Bucket %s already exists", bucketName)
		} else {
			log.Fatalln(err)
		}
	}

	// upload a file
	objectName := "UVA matlab file"
	filePath := "RR/UVA0052_rr.mat"
	contentType := "text/plain"

	n, err := minioClient.FPutObject(bucketName, objectName,
		filePath, minio.PutObjectOptions{ContentType: contentType})

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)

}

func download() {

	endpoint := "minio.uvadcos.io"
	accessKey := "minioadmin"
	secretKey := "miniosecret"

	// initialize client object
	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		//
		log.Fatalln(err)
	}

	// list objects in iTHRIV bucket
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := minioClient.ListObjects("ithriv", "", false, doneCh)

	for object := range objectCh {
		if object.Err != nil {
			log.Fatalln(object.Err)
		}

		log.Printf("Found Object:\n\tName: %s\n\tSize: %d\n\tMD5: %s\n\tLastModified: %s",
			object.Key, object.Size, object.ETag, object.LastModified)
	}

	// get object
	err = minioClient.FGetObject("ithriv", "UVA matlab file",
		"matlabfile.m", minio.GetObjectOptions{})

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Downloaded the UVA matlab file")

}

func BenchmarkMinioupload(b *testing.B) {

	endpoint := "minio.uvadcos.io"
	accessKey := "minioadmin"
	secretKey := "miniosecret"

	// initialize client object
	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		log.Fatalln(err)
	}

	bucketName := "test"

	err = minioClient.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(bucketName)
		if errBucketExists == nil && exists {
			log.Printf("Bucket %s already exists", bucketName)
		} else {
			log.Fatalln(err)
		}
	}

	// upload a file
	objectName := "test1"
	//filePath       := "UVA Holter RR Matlab.zip"
	filePath := "protege.pdf"

	//progressReader := strings.NewReader("Progress Status")

	// open a reader for the
	objectReader, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}

	fileStat, _ := objectReader.Stat()

	options := minio.PutObjectOptions{
		UserMetadata: map[string]string{"id": "ors:ithriv/project", "type": "Dataset"},
		ContentType:  "application/octet-stream",
		NumThreads:   4,
	}
	//options := minio.PutObjectOptions{}

	n, err := minioClient.PutObject(bucketName, objectName, objectReader, fileStat.Size(), options)

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)

}
