package server

import (
	"bytes"
	"net/http"
	"errors"
	"fmt"
//	"log"

	"io/ioutil"
	"github.com/buger/jsonparser"
	"encoding/json"
)


var MDS_URI = "ors.uvadcos.io"

var (
	ErrRequestInit = errors.New("Failed to Create HTTP Request")
	ErrRequestFailure = errors.New("Failed to Complete HTTP Request")
	ErrIdentifierMissingID = errors.New("Identifier Missing ID")
	ErrIdentifierMissingNamespace = errors.New("Identifier Missing Namespace")
	ErrIdentifierMissingMetadata = errors.New("Identifier Requires non Null metadata")
	ErrIdentifierInvalidMetadata = errors.New("Identifier has invalid Metadata")
	ErrMDSOperation = errors.New("MDS API Operation Not Successfull")
	ErrIdentifierDistributionMissing = errors.New("Identifier has no Download Objects")
	ErrIdentifierDistributionMalformed = errors.New("Identifier has malformed Download Property")
)


type Identifier struct {
	ID	string
	Namespace	string
	Metadata	[]byte
}

func (i *Identifier)Post() (err error) {

	if i.ID == "" {
		return ErrIdentifierMissingID
	}

	if len(i.Metadata) == 0 {
		return ErrIdentifierMissingMetadata
	}

	url := "http://" + MDS_URI + "/" + i.ID

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, bytes.NewReader(i.Metadata))
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestInit, err.Error())
	}

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
	}

	if response.StatusCode != 201 {
		// read out response
		return ErrMDSOperation
	}

	r, err := ioutil.ReadAll(response.Body)
	log.Printf("PostedIdentifier\tStatusCode: %d\tBody: %s", response.StatusCode, string(r))

	return

}

func (i *Identifier)Mint() (err error) {

	if i.Namespace == "" {
		return ErrIdentifierMissingNamespace
	}

	if len(i.Metadata) == 0 {
		return ErrIdentifierMissingMetadata
	}

	url := "http://" + MDS_URI + "/shoulder/" + i.Namespace

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, bytes.NewReader(i.Metadata))
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestInit, err.Error())
	}

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
	}

	if response.StatusCode != 201 {
		return ErrMDSOperation
	}

	r, err := ioutil.ReadAll(response.Body)

	mintedId, err := jsonparser.GetString(r, "@id")

	if err != nil {
		return
	}

	i.ID = mintedId

	return


}

func (i *Identifier)Delete() (err error) {

	if i.ID == "" {
		return ErrIdentifierMissingID
	}

	url := "http://" + MDS_URI + "/" + i.ID

	client := &http.Client{}

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestInit, err.Error())
	}

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
	}

	r, err := ioutil.ReadAll(response.Body)

	if response.StatusCode != 200 {
		// read out response
		return ErrMDSOperation
	}

	i.Metadata = r

	return

}

func (i *Identifier)Update() (err error) {

	if i.ID == "" {
		return ErrIdentifierMissingID
	}

	if len(i.Metadata) == 0 {
		return ErrIdentifierMissingMetadata
	}

	url := "http://" + MDS_URI + "/" + i.ID

	client := &http.Client{}

	req, err := http.NewRequest("PUT", url, bytes.NewReader(i.Metadata))
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestInit, err.Error())
	}

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
	}

	r, err := ioutil.ReadAll(response.Body)
	if response.StatusCode != 201 {
		// read out response
		return ErrMDSOperation
	}

	i.Metadata = r

	return

}

func (i *Identifier)Get() (err error) {

	if i.ID == "" {
		return ErrIdentifierMissingID
	}

	url := "http://" + MDS_URI + "/" + i.ID

	client := &http.Client{}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		err = fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
		return
	}

	response, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("%w\t%s", ErrRequestFailure, err.Error())
		return
	}

	r, err := ioutil.ReadAll(response.Body)

	if response.StatusCode != 200 {
		// read out response
		return fmt.Errorf("Message: %w\tStatusCode: %d\tResponse: %s", ErrMDSOperation, response.StatusCode, string(r))
	}

	i.Metadata = r
	return

}

func (i *Identifier)getDownloads() (downloads []Download, err error) {

	downloadJson, dtype, _, err := jsonparser.Get(i.Metadata, "distribution")

	if err != nil {
		return
	}

	switch dtype {

		case jsonparser.Object:
			var d Download
			err = json.Unmarshal(downloadJson, &d)
			downloads = append(downloads, d)

		case jsonparser.Array:
			err = json.Unmarshal(downloadJson, &downloads)

		case jsonparser.NotExist:
			err = ErrIdentifierDistributionMissing

		case jsonparser.Null:
			err = ErrIdentifierDistributionMissing

		default:
			err = ErrIdentifierDistributionMalformed
	}



	return
}

type Download struct {
	ID string	    `json:"@id"`
	Type string	    `json:"@type"`
	Name string	    `json:"name"`
	ContentURL string   `json:"contentURL"`
}


/*
func (d *Download)GetFile() (err error) {

		contentURL := strings.TrimPrefix(download.ContentURL, "s3a://")
		contentURLSplit := strings.SplitN(contentURL, "/", 3)
		bucket = contentURLSplit[1]
		key = contentURLSplit[2]

	return
}
*/
