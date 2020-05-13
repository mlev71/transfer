package server

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"
)

func TestS3(t *testing.T) {

	testS3 := S3{
		AccessKey: "minioadmin",
		SecretKey: "miniosecret",
		Endpoint: "http://localhost:9000",
	}

	var bucketName = "unittest"

	t.Run("CreateBucket", func(t *testing.T){
		err = testS3.CreateBucket(bucketName)
		if err != nil {
			t.Fatalf("ListBucketsError: %w", err)
		}
	})
	t.Run("ListBuckets", func(t *testing.T){

		buckets, err := testS3.ListBuckets()

		if err != nil {
				t.Fatalf("ListBucketsError: %w", err)
		}

		t.Logf("ListBuckets: %s", strings.Join(buckets, " , "))

	})
	t.Run("DeleteBucket", func(t *testing.T){})
	t.Run("CreateObject", func(t *testing.T){})
	t.Run("GetObject", func(t *testing.T){})
	t.Run("GetObjectInfo", func(t *testing.T){})
	t.Run("DeleteObject", func(t *testing.T){})

}
