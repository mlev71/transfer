package server

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestIdentifier(t *testing.T){


	t.Run("GetDownloads", func(t *testing.T){
		metadata := []byte(`{
		"@id": "ark:99999/testdataset",
		"@type": "Dataset",
		"name": "Test Dataset",
		"distribution": {
			"@id": "ark:99999/testdownload",
			"name": "Test Download",
			"contentURL": "s3a://bucket/key"
			}
		}`)

		id := Identifier{Metadata: metadata}

		downloads, err := id.GetDownloads()

		if err != nil {
			t.Fatalf("")
		}

		assert.Equal(len(downloads), 1)
		assert.Equal(downloads[0].ID, "ark:99999/testdownload")
		assert.Equal(downloads[0].ContentURL, "s3a://bucket/key")

	})

}


func TestDownload() {

	// set up create namespace in MDS

	// create a dataset

	// create a datadownload

	// get the dataset

	// get the datadownload

	//

}
