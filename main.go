package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/tidwall/gjson"
	"github.com/urfave/negroni"
	"log"
	"net/http"

	"bufio"
	"io/ioutil"
	"time"
	//"strings"
	"github.com/minio/minio-go"
	"io"
	"mime/multipart"
)

var ORS_MDS = "ors.uvadcos.io"
var MINIO_ENDPOINT = "minionas.uvadcos.io"
var MINIO_ACCESSKEY = "breakfast"
var MINIO_SECRETKEY = "breakfast"
var DEFAULT_NAMESPACE = "ark:99999"
var DEFAULT_BUCKET = "default"

func init() {

	// initialize client object
	minioClient, err := minio.New(MINIO_ENDPOINT, MINIO_ACCESSKEY, MINIO_SECRETKEY, false)
	if err != nil {
		// crash the program if client cannot initialize
		log.Fatalln("STARTUP: Minio Client Error " + err.Error())
	}

	var bucketName = "default"
	err = minioClient.MakeBucket(bucketName, "us-east-1")

}

func main() {

	r := mux.NewRouter().StrictSlash(false)
  r.HandleFunc("/{prefix}/{suffix}", DownloadHandler)
	r.HandleFunc("/upload", UploadHandler)

	n := negroni.New()
	n.UseHandler(r)

	log.Fatal(http.ListenAndServe(":8080", n))

}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	var datasetGUID, downloadGUID string
	var bucket, key, namespace string
	log.Println("Starting Parse Request")

	w.Header().Set("Content-Type", "application/json")

	err := r.ParseMultipartForm(2 << 20)
	if err != nil {
		http.Error(w, `{"error": "`+err.Error()+`", "message": "Failed to Parse Multipart Form"}`, 500)
		return
	}

	objectFile, objectFileHeader, err := r.FormFile("object")

	if err != nil {
		http.Error(w, `{"error": "`+err.Error()+`", "message": "Multipart Form missing object file upload"}`, 400)
		return
	}

	datasetMetadata := r.PostFormValue("metadata")

	if datasetMetadata == "" {
		http.Error(w, `{"error": "`+err.Error()+`", "message": "Multipart Form missing object metadata"}`, 400)
		return
	}

	namespace = r.PostFormValue("namespace")
	if namespace == "" {
		namespace = DEFAULT_NAMESPACE
	}

	bucket = r.PostFormValue("bucket")
	if bucket == "" {
		bucket = DEFAULT_BUCKET
	}

	key = r.PostFormValue("key")
	if key == "" {
		key = objectFileHeader.Filename
	}

	// mint the dataset identifier
	mintDataset, err := MintIdentifier(namespace, []byte(datasetMetadata))
	datasetGUID = gjson.Get(string(mintDataset), "created").String()

	if err != nil {
		http.Error(w, "Failed to Create Identifier: "+err.Error(), 500)
		return
	}

	// mint the DataDownload Identifier
	now, _ := time.Now().MarshalText()

	//dataDownload["@context"] = "http://schema.org/"
	dataDownload := map[string]string{
    "@context":     "http://schema.org/",
    "@type":        "DataDownload",
		"name":         objectFileHeader.Filename,
		"dateUploaded": string(now),
		"dataset":      datasetGUID,
	}
	dataDownloadEncoded, err := json.Marshal(dataDownload)

	if err != nil {
		http.Error(w, "Error Encoding DataDownload Metadata to JSON: "+err.Error(), 500)
		return
	}

	mintDownload, err := MintIdentifier("ark:99999", dataDownloadEncoded)
	downloadGUID = gjson.Get(string(mintDownload), "created").String()
	dataDownload["@id"] = downloadGUID

	// Write Object to Minio
	// create a minio client
	minioClient, err := minio.New(MINIO_ENDPOINT, MINIO_ACCESSKEY, MINIO_SECRETKEY, false)

	if err != nil {
		http.Error(w, "Error Creating Minio Client: "+err.Error(), 500)
		return
	}

	// upload object to minio
	_, err = minioClient.PutObject(
		bucket,
		objectFileHeader.Filename,
		objectFile,
		objectFileHeader.Size,
		minio.PutObjectOptions{UserMetadata: map[string]string{"id": downloadGUID, "type": "DataDownload", "dataset": datasetGUID}},
	)

	if err != nil {
		http.Error(w, `{"error": "`+err.Error()+`", "message", "Failed to Upload Object to Minio"}`, 500)
		return
	}

	// Update Dataset
	_, err = json.Marshal(map[string]interface{}{
		"distribution": map[string]interface{}{
			"@id":   downloadGUID,
			"@type": "DataDownload",
			"name":  objectFileHeader.Filename,
		},
	})

	// Update DataDownload with S3 Path
	downloadUpdate, err := json.Marshal(map[string]interface{}{
		"contentURL": "s3a://" + MINIO_ENDPOINT + "/" + bucket + "/" + key,
	})

	_, err = UpdateIdentifier(downloadGUID, downloadUpdate)

	if err != nil {
		http.Error(w, "Error Updating Download GUID Identifier with S3 URL: "+err.Error(), 500)
		return
	}

	w.WriteHeader(200)
	w.Write([]byte(`{"status": "success", "namespace": "` + namespace + `", "bucket": "` +
		bucket + `", "metadata": ` + datasetMetadata + `, "dataset": "` +
		datasetGUID + `", "download": "` + downloadGUID + `"}`))
}

func DownloadHandler(w http.ResponseWriter, r *http.Request) {

	// read in path variables
	vars := mux.Vars(r)
	guid := vars["prefix"] + "/" + vars["suffix"]

	// get the identifier metadata
	bucket, key, err := QueryDownload(guid)

	if err != nil {
		http.Error(w, "Error Finding Object: "+err.Error(), 500)
		return
	}

	// construct a multipart form
	formWriter := multipart.NewWriter(w)
	w.Header().Set("Content-Type", formWriter.FormDataContentType())

	objectWriter, err := formWriter.CreateFormFile("object", key)
	if err != nil {
		http.Error(w, "Error Forming MultipartResponse:"+err.Error(), 500)
		return
	}

	// create a minio client
	minioClient, err := minio.New(MINIO_ENDPOINT, MINIO_ACCESSKEY, MINIO_SECRETKEY, false)
	if err != nil {
		http.Error(w, "Error Creating Minio Client: "+err.Error(), 500)
		return
	}

	// get minio object
	object, err := minioClient.GetObject(bucket, key, minio.GetObjectOptions{})

	if err != nil {
		http.Error(w, "Failed to Get Minio Object: "+err.Error(), 500)
		return
	}

	go io.Copy(objectWriter, object)
	w.WriteHeader(200)
}

func UpdateHandler(w http.ResponseWriter, r *http.Request) {}

func UploadStreaming(w http.ResponseWriter, r *http.Request) {
	var datasetGUID, downloadGUID string
	var bucket = "default"

	// TODO: Cancelable Request cleanup
	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify

		// delete datadownload

		// delete the minio object

		// delete the identifier for the dataset
		log.Println("Request cancelled cleaning up resources")

	}()

	// get multipart reader from request
	multipartFormReader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Error Streaming MultipartForm: "+err.Error(), 400)
		return
	}

	// progress the reader
	p, err := multipartFormReader.NextPart()
	if err != nil {
		http.Error(w, "Error Streaming MultipartForm: "+err.Error(), 400)
		return
	}

	// read the identifier metadata from the form
	if p.FormName() == "metadata" {

		// read all metadata
		datasetMetadata, err := ioutil.ReadAll(p)
		created, err := MintIdentifier("ark:99999", datasetMetadata)

		if err != nil {
			http.Error(w, "Failed to Create Identifier: "+err.Error(), 500)
			return
		}

		// set @id
		datasetGUID = gjson.Get(string(created), "created").String()
		//updatedMetadata := sjson.Set(string(metadata), "@id", guid.String())

	} else {
		http.Error(w, "Failed to extract metadata: "+err.Error(), 400)
		return
	}

	// read the next part of the form
	p, err = multipartFormReader.NextPart()
	if err != nil {
		http.Error(w, "Error Streaming MultipartForm: "+err.Error(), 400)
		return
	}

	// create identifier and upload content to minio
	if p.FormName() == "data" {
		// create a data download identifier for this dataset
		dataDownload := make(map[string]string)
		dataDownload["name"] = p.FileName()
		dataDownload["@context"] = "http://schema.org/"

		now, _ := time.Now().MarshalText()
		dataDownload["dateUploaded"] = string(now)

		dataDownloadEncoded, err := json.Marshal(dataDownload)

		if err != nil {
			http.Error(w, "Error Encoding DataDownload Metadata to JSON: "+err.Error(), 500)
			return
		}

		mintResponse, err := MintIdentifier("ark:99999", dataDownloadEncoded)

		if err != nil {
			http.Error(w, "Error Minting DataDownload Identifier: "+err.Error(), 500)
			return
		}

		downloadGUID = gjson.Get(string(mintResponse), "created").String()
		dataDownload["@id"] = downloadGUID

		// write upload stream to minio
		// create a minio client
		minioClient, err := minio.New(MINIO_ENDPOINT, MINIO_ACCESSKEY, MINIO_SECRETKEY, false)

		if err != nil {
			http.Error(w, "Error Creating Minio Client: "+err.Error(), 500)
			return
		}

		// upload object to minio
		opts := minio.PutObjectOptions{UserMetadata: dataDownload}
		_, err = minioClient.PutObject(bucket, p.FileName(), bufio.NewReader(p), -1, opts)

		if err != nil {
			http.Error(w, "Error Streaming MultipartForm: "+err.Error(), 500)
			return
		}

	} else {
		http.Error(w, "Failed to Find 'data' Element in MultipartForm: "+err.Error(), 400)
		return
	}

	// Update DataDownload with S3 Path
	downloadUpdate, err := json.Marshal(map[string]interface{}{
		"contentURL": "s3a://" + MINIO_ENDPOINT + "/" + bucket + "/" + p.FileName(),
	})

	_, err = UpdateIdentifier(downloadGUID, downloadUpdate)

	if err != nil {
		http.Error(w, "Error Updating Download GUID Identifier with S3 URL: "+err.Error(), 500)
		return
	}

	// Identifier to Have additional distribution
	datasetUpdate, err := json.Marshal(map[string]interface{}{
		"distribution": []string{downloadGUID},
	})

	_, err = UpdateIdentifier(datasetGUID, datasetUpdate)

	if err != nil {
		http.Error(w, "Error Updating DatasetGUID with DataDownload: "+err.Error(), 500)
		return
	}

	response := make(map[string]interface{})
	response["dataset"] = datasetGUID
	response["download"] = downloadGUID

	responseJSON, _ := json.Marshal(response)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(responseJSON)
}

func DownloadStreaming(w http.ResponseWriter, r *http.Request) {}
