package transfer

import (
	"bytes"
	"errors"

	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"encoding/json"
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

	contentURLSplit := strings.SplitN(contentURL, "/", 2)

	if len(contentURLSplit) != 3 {
		err = errors.New("Content URL Not Split Correctly: " + contentURL)
		return
	}

	bucket = contentURLSplit[1]
	key = contentURLSplit[2]

	return

}

func PostIdentifier(id string, metadata []byte) (r []byte, err error) {

	// parse id
	var u url.URL
	u.Scheme = "http"
	u.Host = "ors.uvadcos.io"
	u.Path = id

	r, err = CreateIdentifier(u.String(), bytes.NewReader(metadata))
	return
}

func MintIdentifier(namespace string, metadata []byte) (r []byte, err error) {

	var u url.URL
	u.Scheme = "http"
	u.Host = "ors.uvadcos.io"
	u.Path = "shoulder/" + namespace

	r, err = CreateIdentifier(u.String(), bytes.NewReader(metadata))
	return
}

func UpdateIdentifier(id string, update []byte) (r []byte, err error) {

	// parse id
	var u url.URL
	u.Scheme = "http"
	u.Host = "ors.uvadcos.io"
	u.Path = id

	client := &http.Client{}

	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(update))
	if err != nil {
		return
	}

	response, err := client.Do(req)
	if err != nil {
		return
	}

	r, err = ioutil.ReadAll(response.Body)
	if response.StatusCode != 200 {
		// read out response
		err = errors.New("Update Identifier Failed: Status != 200")
	}

	return

}

func CreateIdentifier(url string, metadata io.Reader) (r []byte, err error) {

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, metadata)
	if err != nil {
		return
	}

	response, err := client.Do(req)
	if err != nil {
		return
	}

	r, err = ioutil.ReadAll(response.Body)
	if response.StatusCode != 201 {
		// read out response
		err = errors.New("Post Identifier Failed: Status != 201")
	}

	return
}

func GetIdentifier(id string) (r []byte, err error) {

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

	r, err = ioutil.ReadAll(response.Body)
	if response.StatusCode != 200 {
		// read out response
		err = errors.New("GET Identifier Failed: Status != 200")
	}

	return
}

func DeleteIdentifier(id string) (r []byte, err error) {

	var u url.URL
	u.Scheme = "http"
	u.Host = "ors.uvadcos.io"
	u.Path = id

	client := &http.Client{}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return
	}

	response, err := client.Do(req)
	if err != nil {
		return
	}

	r, err = ioutil.ReadAll(response.Body)
	if response.StatusCode != 200 {
		// read out response
		err = errors.New("DELETE Identifier Failed: Status != 200")
	}

	return

}
