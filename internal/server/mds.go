package mds

import (
	"bytes"
	"errors"

	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type GUID struct {
	Context interface{} `json:"@context"`
	ID      string      `json:"@id"`
	Type    string      `json:"@type"`
	Name    string      `json:"name"`
	URL     string      `json:"url"`
}

type Dataset struct {
	GUID
	Distribution []DataDownload `json:"distribution"`
}

type DataDownload struct {
	GUID
	ContentURL string `json:"contentURL"`
}

func QueryDownload(id string) (bucket string, key string, err error) {

	var u url.URL
	u.Scheme = "http"
	u.Host = "ors.uvadcos.io"
	u.Path = id

	client := &http.Client{}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return
	}

	response, err := client.Do(req)
	if err != nil {
		return
	}
	if response.StatusCode != 200 {
		// read out response
		err = errors.New("GET Identifier Failed: Status != 200")
		return
	}

	r, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	var identifier GUID

	err = json.Unmarshal(r, &identifier)
	if err != nil {
		return
	}

	if identifier.Type != "DataDownload" {
		err = errors.New("Type is not Data Download")
		return
	}

	var download DataDownload
	err = json.Unmarshal(r, &download)

	contentURL := strings.TrimPrefix(download.ContentURL, "s3a://")

	contentURLSplit := strings.SplitN(contentURL, "/", 3)

	bucket = contentURLSplit[1]
	key = contentURLSplit[2]

	return

}
