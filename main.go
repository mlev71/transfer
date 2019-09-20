package main

import (
  "github.com/minio/minio-go"
  "log"
  "os"

  "net/http"
  "github.com/gorilla/mux"
  "github.com/urfave/negroni"

  "testing"
  "encoding/json"
  "io"
  //"io/ioutil"
  "bufio"
  "bytes"
  //"strings"
)

var ENDPOINT = "minionas.uvadcos.io"
var ACCESSKEY = "breakfast"
var SECRETKEY = "breakfast"

func init() {

  // initialize client object
  minioClient, err := minio.New(ENDPOINT, ACCESSKEY, SECRETKEY, false)
  if err != nil {
    // crash the program if client cannot initialize
    log.Fatalln("STARTUP: Minio Client Error " + err.Error())
  }

  var bucketName = "default"
  err = minioClient.MakeBucket(bucketName, "us-east-1")


}

func main () {

  r := mux.NewRouter().StrictSlash(false)
  n := negroni.New()

  r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    if r.Method == "POST" {
      UploadHandler(w, r)
    } else {
      http.Error(w, "Method Not Allowed" , 403)
    }
    return
  })


  r.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
    if r.Method == "POST" {
      UploadStreaming(w, r)
    } else {
      http.Error(w, "Method Not Allowed" , 403)
    }
    return
  })

  n.UseHandler(r)
  log.Fatal(http.ListenAndServe(":8080", n))

}


func UploadStreaming(w http.ResponseWriter, r *http.Request) {

  // get multipart reader from request
  //var metadataEncoded []byte
  //var buf = make([]byte, 256)

  var metadataEncoded bytes.Buffer
  var dataBuffer bytes.Buffer


  multipartFormReader, err := r.MultipartReader()
  if err != nil {
    http.Error(w, "Error Streaming MultipartForm: " + err.Error(), 400)
    return
  }

  var dataFileName string
  var dataSize int64

  for {
    p, err := multipartFormReader.NextPart()

    if err == io.EOF {
      break
    }

    if err != nil {
      http.Error(w, "Error Streaming MultipartForm: " + err.Error(), 400)
      return
    }

    log.Printf("READING PART")
    log.Printf("File Name: %s", p.FileName())
    log.Printf("Form Name: %s", p.FormName())

    if p.FormName() == "metadata" {

      // read all at once
      //metadataEncoded, err = ioutil.ReadAll(p)

      // read split up in to chunks of buf.Len()
      /*
      var n int
      for {
        n, err = p.Read(buf)
        if err == io.EOF {
          err = nil
          break
        }
        log.Println("READ CHUNK: " + string(buf[:n]))
      }

      if err != nil {
        http.Error(w, "Error Streaming Metadata: " + err.Error(), 400)
        return
      }
    }
    */

      // read into a bytes.Buffer using ReadFrom(io.Reader)

      n, err := metadataEncoded.ReadFrom(p)

      if err != nil {
        http.Error(w, "Error Streaming MultipartForm: " + err.Error(), 500)
        return
      }

      log.Printf("METADATA READ %d BYTES", n)

    }

    if p.FormName() == "data" {

      dataFileName = p.FileName()
      dataSize, err = dataBuffer.ReadFrom(p)

      if err != nil {
        http.Error(w, "Error Streaming MultipartForm: " + err.Error(), 500)
        return
      }

      log.Printf("DATA READ %d BYTES", dataSize)


    }
  }


  /*
  // decode json value
  metadata := make(map[string]interface{})
  err = json.Unmarshal(metadataEncoded, &metadata)

  if err != nil {
    http.Error(w, "Failed to Unmarshal JSON: " + err.Error(), 500)
    return
  }
  */


  // write to minio
  minioClient, err := minio.New(ENDPOINT, ACCESSKEY, SECRETKEY, false)

  if err != nil {
    http.Error(w, "Error Creating Minio Client: " + err.Error(), 500)
    return
  }


  dataReader := bufio.NewReader(&dataBuffer)

  // upload object to minio
  uploaded, err := minioClient.PutObject("default", dataFileName, dataReader, dataSize, minio.PutObjectOptions{})

  log.Printf("MINIO UPLOADED %d bytes OF %d bytes", uploaded, dataSize)

  if err != nil {
    http.Error(w, "Minio Put Object Operation Failed: " + err.Error(), 500)
    return
  }

  /*
  if uploaded != dataSize {
    http.Error(w, "Uploaded " + string(uploaded) + " bytes, but object is " + string(dataSize) + " bytes", 500 )
    return
  }
  */

  response := make(map[string]interface{})
  response["uploaded"] = uploaded

  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(200)

  b, _ := json.Marshal(response)
  w.Write(b)
}


func UploadHandler(w http.ResponseWriter, r *http.Request) {

  // parse the multipart form
  err := r.ParseMultipartForm(32<<20)

  if err != nil {
    http.Error(w, "Parsing Multipart Form Failed: " + err.Error(), 500)
    return
  }

  // access metadata
  metadataEncoded := r.FormValue("metadata")

  if metadataEncoded == "" {
    http.Error(w, "Upload Metadata is Nill", 400)
    return
  }

  // decode json value
  metadata := make(map[string]interface{})
  err = json.Unmarshal( []byte(metadataEncoded), &metadata)

  if err != nil {
    http.Error(w, "Failed to Unmarshal JSON: " + err.Error(), 500)
    return
  }


  // get project
  project := r.FormValue("project")

  if project == "" {
    project = "default"
  }


  // access data
  dataFile, dataHeader, err := r.FormFile("data")
  //_, _, err = r.FormFile("data")

  if err != nil {
    http.Error(w, "Error Parsing Data blob from Request: " + err.Error(), 400)
    return
  }

  // write to minio
  minioClient, err := minio.New(ENDPOINT, ACCESSKEY, SECRETKEY, false)

  if err != nil {
    http.Error(w, "Error Creating Minio Client: " + err.Error(), 500)
    return
  }

  // write object


  // upload object to minio
  n, err := minioClient.PutObject(project, dataHeader.Filename, dataFile, dataHeader.Size, minio.PutObjectOptions{})

  if err != nil {
    http.Error(w, "Minio Put Object Operation Failed: " + err.Error(), 500)
    return
  }

  if n != dataHeader.Size {
    http.Error(w, "Uploaded " + string(n) + " bytes, but object is " + string(dataHeader.Size) + " bytes", 500 )
    return
  }

  response := make(map[string]interface{})
  response["uploaded"] = n

  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(200)

  b, _ := json.Marshal(response)
  w.Write(b)


}

/*
func upload() {

  // upload a file
  objectName  := "UVA matlab file"
  filePath    := "RR/UVA0052_rr.mat"
  contentType := "text/plain"

  n, err := minioClient.FPutObject(bucketName, objectName,
    filePath, minio.PutObjectOptions{ContentType:contentType})

  if err != nil {
    log.Fatalln(err)
  }

  log.Printf("Successfully uploaded %s of size %d\n", objectName, n)

}
*/

func DownloadHandler() {}

func download() {

  endpoint  := "minio.uvadcos.io"
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
  err =  minioClient.FGetObject("ithriv", "UVA matlab file",
    "matlabfile.m",minio.GetObjectOptions{})

  if err != nil {
    log.Fatal(err)
  }

  log.Println("Downloaded the UVA matlab file")

}



func BenchmarkMinioupload(b *testing.B) {

  endpoint  := "minio.uvadcos.io"
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
  objectName     := "test1"
  //filePath       := "UVA Holter RR Matlab.zip"
  filePath       := "protege.pdf"

  //progressReader := strings.NewReader("Progress Status")

  // open a reader for the
  objectReader, err := os.Open(filePath)
  if err != nil {
    log.Fatalln(err)
  }

  fileStat, _ := objectReader.Stat()


  options :=  minio.PutObjectOptions{
      UserMetadata: map[string]string{"id": "ors:ithriv/project", "type": "Dataset"},
      ContentType: "application/octet-stream",
      NumThreads:  4,
      }
  //options := minio.PutObjectOptions{}

  n, err := minioClient.PutObject( bucketName, objectName, objectReader, fileStat.Size(), options)

  if err != nil {
    log.Fatalln(err)
  }

  log.Printf("Successfully uploaded %s of size %d\n", objectName, n)

}
