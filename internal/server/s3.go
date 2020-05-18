package server

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"log"
	"sync"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"os"
	//    "net/http"
)


var (
	s3Logger = zerolog.New(os.Stderr).With().Timestamp().Str("backend", "s3").Logger()
)


type S3 struct {
	AccessKey  string
	SecretKey  string
	Endpoint   string
	DisableSSL bool
}

func (s *S3) session() (svc *s3.S3, err error) {

	cred := credentials.NewStaticCredentials(
		s.AccessKey,
		s.SecretKey,
		"",
	)

	config := aws.NewConfig().
		WithEndpoint(s.Endpoint).
		WithDisableSSL(s.DisableSSL).
		WithS3ForcePathStyle(true).
		WithRegion("us-east-1").
		WithCredentials(cred)

	sess, err = session.NewSession(config)

	svc := s3.New(sess)
	return

}

func (s *S3) ListBuckets() (buckets []string, err error) {

	sess, err := s.session()
	if err != nil {
		s3Logger.Error().
			String("operation", "ListBuckets").
			Err("error", err).
			Msg("Error Constructing Session")

		return
	}


	result, err := sess.ListBuckets(nil)
	if err != nil {
		s3Logger.Error().
			String("operation", "ListBuckets").
			Err("error", err).
			Msg("Error Preforming Operation")

		return
	}


	foundLog := zerolog.Arr()
	for i, bucket := range result.Buckets {
		buckets[i] = bucket.Name
		foundLog = foundLog.Str(bucket.Name)
	}


	s3Logger.Info().
		String("operation", "ListBuckets").
		Array("buckets", foundLog)
		Msg("Success")

	return
}

func (s *S3) CreateBucket(name string) (err error) {

	var bucketInput *s3.CreateBucketInput
	bucketInput = bucketInput.SetBucket(name)

	sess, err := s.session()
	if err != nil {
		return
	}


	bucketOutput, err := sess.CreateBucket(bucket)
	// log the bucket output

	s3Logger.Info().
		String("bucketName", name)
		String("output", bucketOutput.String()).
		Msg("Successfully created bucket")
)

	return
}

func (s *S3) GetObjectInfo() (err error) {
	return
}

func (s *S3) GetObject() (err error) {
	return
}

func (s *S3) UploadObject() (err error) {
	return
}

func (s *S3) DeleteObject() (err error) {
	return
}


func Upload(bucketName string, objectName string, object io.Reader) (err error) {
	sess, err := s3Session()

	if err != nil {
		return
	}

	// Create an uploader with the session and custom options
	//uploader := s3manager.NewUploader(sess)
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.Concurrency = 8
		// u.PartSize = 10 * 1024 * 1024 // 64MB per part
	})

	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Body:   object, //io.Reader
		//Metadata: , //map[string]*string
	}

	_, err = uploader.Upload(uploadInput)

	return
}

func DownloadBasic(bucketName string, objectName string, object io.WriterAt) (err error) {
	sess, err := s3Session()
	if err != nil {
		return
	}

	downloader := s3manager.NewDownloader(sess)

	_, err = downloader.Download(object, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})

	if err != nil {
		return
	}

	return
}

func DownloadMultipart(bucketName string, objectName string, object io.WriterAt) (err error) {
	sess, err := s3Session()
	if err != nil {
		return
	}

	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.Concurrency = 10
		return
	})

	_, err = downloader.Download(object, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})

	if err != nil {
		return
	}

	return
}

func DownloadRange(bucketName string, objectName string, byteStart, byteEnd int) (result *s3.GetObjectOutput, err error) {

	sess, err := s3Session()
	if err != nil {
		return
	}

	svc := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", byteStart, byteEnd)),
	}

	result, err = svc.GetObject(input)
	return

}

type ObjectSlice struct {
	ID         int
	Body       bytes.Buffer
	S3         *s3.S3
	bucketName string
	objectName string
	rangeStart int
	rangeEnd   int
}

func readSlice(input chan ObjectSlice, output chan ObjectSlice, wg *sync.WaitGroup) {
	// TODO: turn into looping worker pool
	// i.e.
	//for{
	//	    if <-done { break }
	// }
	slice := <-input

	object := &s3.GetObjectInput{
		Bucket: aws.String(slice.bucketName),
		Key:    aws.String(slice.objectName),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", slice.rangeStart, slice.rangeEnd-1)),
	}

	result, err := slice.S3.GetObject(object)

	if err != nil {
		log.Panicf("Failed to Get Object from S3: %s", err.Error())
	}

	defer result.Body.Close()

	// copy contents of result to response
	// not workiung with bytes.Buffer
	//  n, err := io.Copy(response.Body, result.Body)
	n, err := slice.Body.ReadFrom(result.Body)
	if err != nil {
		log.Panicf("Failed to Copy IO from s3 to result: %s", err.Error())
	}

	log.Printf("Copied Buffer Successfully Length: %d", n)

	// send response into messages channel
	output <- slice
	wg.Done()
}

func DownloadConcurrent(bucketName string, objectName string, chunkChan chan bytes.Buffer) {
	var N_GOROUTINES int = 4
	var chunkWidth int = 10000

	input := make(chan ObjectSlice, N_GOROUTINES)
	output := make(chan ObjectSlice, N_GOROUTINES)

	var wg sync.WaitGroup

	sess, err := s3Session()
	if err != nil {
		return
	}

	svc := s3.New(sess)

	// TODO break this section into seperate Iterate this section

	for i := 0; i < N_GOROUTINES; i++ {
		slice := ObjectSlice{
			ID:         i,
			S3:         svc,
			bucketName: bucketName,
			objectName: objectName,
			rangeStart: i * chunkWidth,
			rangeEnd:   i*chunkWidth + chunkWidth,
		}

		input <- slice

		wg.Add(1)
		go readSlice(input, output, &wg)
	}

	wg.Wait()

	slices := make([]ObjectSlice, N_GOROUTINES, N_GOROUTINES)
	var num int = 0

	for sl := range output {
		slices[sl.ID] = sl
		log.Printf("Added Slice to Buffer ID: %d", sl.ID)
		num++
		// slices = append(slices, sl)

		if num > 3 {
			break
		}
	}

	// peice together peices
	var chunkBuffer bytes.Buffer

	// wait group for writing to buffer
	for _, sl := range slices {

		// write out the contents of the buffers
		n, err := sl.Body.WriteTo(&chunkBuffer)
		if err != nil {
			return
		}

		// print out slice
		log.Printf("Message Copied\tWorker: %d\tBytesCopied: %d\tStart:%d\tEnd: %d",
			sl.ID, n, sl.rangeStart, sl.rangeEnd)
	}

	chunkChan <- chunkBuffer
}
