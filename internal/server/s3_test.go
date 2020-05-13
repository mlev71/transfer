package server

import (
    "io/ioutil"
    "github.com/aws/aws-sdk-go/aws"
    "testing"
    "strings"
    "bytes"
)

func TestS3ListBuckets(t *testing.T) {

    err := ListBuckets()
    if err != nil {
	t.Fatalf("Failed to List Buckets\nError: %s", err.Error())
    }

}

func TestS3Upload(t *testing.T) {

    t.Run("Basic", func(t *testing.T) {
	// open a file as an io.Reader
	reader := strings.NewReader("Hello This is a Test File")
	bucketName := "test"
	objectName := "test.txt"

	// upload to a bucket, random filename
	err := Upload(bucketName, objectName, reader)

	if err != nil {
	    t.Fatalf("Failed to Upload File: %s", err.Error())
	}
    })

}

func TestS3Download(t *testing.T) {

    bucketName := "prevent"
    objectName := "testSample.csv"

    t.Run("Multipart", func(t *testing.T) {
	var init []byte
	dl := aws.NewWriteAtBuffer(init)

	// synchronous download
	err := DownloadMultipart(bucketName, objectName, dl)
	if err != nil {
	    t.Fatalf("Failed to Download File: %s", err.Error())
	}

	// only writes out whole chunk
	body := dl.Bytes()
	t.Logf("BODY:\n%s", body)
    })

    t.Run("Range", func(t *testing.T) {
	result, err := DownloadRange(bucketName, objectName, 0, 9)
	if err != nil {
	    t.Fatalf("Failed to Download Part: %s", err.Error())
	}

	// read out whole body
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
	    t.Fatalf("Failed to Read Body: %s", err.Error())
	}
	t.Logf("BODY:\n%s", body)
    })

    t.Run("Concurrent", func(t *testing.T) {

	bucketName := "prevent"
	objectName := "testSample.csv"

	incr := 100
	for i:=1;i<100;i++ {
	    go DownloadRange(bucketName, objectName, (i-1)*incr, i*incr)
	}

    })

}

func TestS3Concurrent(t *testing.T) {

    bucketName := "prevent"
    objectName := "testSample.csv"

    chunkChannel:= make(chan bytes.Buffer)

    go DownloadConcurrent(bucketName, objectName, chunkChannel)

    page := <-chunkChannel
    t.Logf("Recieved Message: %s", page.String())

}
