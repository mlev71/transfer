package server

import (
	"testing"
	"encoding/json"
	"github.com/buger/jsonparser"
	// "github.com/stretchr/testify/assert"
)

func TestIdentifier(t *testing.T){

	metadata := []byte(`{
		"@type": "Dataset",
		"name": "Test Dataset",
		"distribution": {
		"@id": "ark:99999/testdownload",
		"name": "Test Download",
		"contentURL": "s3a://bucket/key"
		}
		}`)

	id := &Identifier{
			Namespace: "ark:99999",
			Metadata: metadata,
	}


	t.Run("Mint", func(t *testing.T){

		err := id.Mint()
		if err != nil {
			t.Fatalf("Failed to Mint Identifier: %s", err.Error())
		}
		t.Logf("Minted Identifier: %s", id.ID)
	})

	t.Run("Post", func(t *testing.T){})

	t.Run("Update", func(t *testing.T){})

	t.Run("Get", func(t *testing.T){})

	t.Run("Delete", func(t *testing.T){})

}

func TestDownload(t *testing.T) {

	t.Run("getDownloads", func(t *testing.T){

		//a := assert.New(t)

		t.Run("Single", func(t *testing.T){

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

		    downloadJson, dtype, _, err := jsonparser.Get(metadata, "distribution")

		    if err != nil {

			t.Fatalf("Failed to Get Distribution: %s", err.Error())
		    }
		    t.Logf("Found Datatype: %+v", dtype)
		    t.Logf("Found Download: %s", string(downloadJson))

		    // unmarshal into a download
		    var d Download
		    err = json.Unmarshal(downloadJson, &d)

		    if err != nil {
			t.Fatalf("Failed to Unmarshal JSON: %s", err.Error())
		    }

		    t.Logf("Download Object ID: %s", d.ID)
		})


		t.Run("Multiple", func(t *testing.T){

		    metadata := []byte(`{
				    "@id": "ark:99999/testdataset",
				    "@type": "Dataset",
				    "name": "Test Dataset",
				    "distribution": [
					{
					"@id": "ark:99999/testdownloadone",
					"name": "Test Download",
					"contentURL": "s3a://bucket/key"
					},
					{
					"@id": "ark:99999/testdownloadtwo",
					"name": "Test Download",
					"contentURL": "s3a://bucket/key"
					}
					]
				    }`)

		    downloadJson, dtype, _, err := jsonparser.Get(metadata, "distribution")

		    if err != nil {

			t.Fatalf("Failed to Get Distribution: %s", err.Error())
		    }
		    t.Logf("Found Datatype: %+v", dtype)
		    t.Logf("Found Download: %s", string(downloadJson))

		    // unmarshal into a download
		    var d []Download
		    err = json.Unmarshal(downloadJson, &d)

		    if err != nil {
			t.Fatalf("Failed to Unmarshal JSON: %s", err.Error())
		    }

		    t.Logf("Download Object ID: %+v", d)
		})

		/*
		id := Identifier{Metadata: metadata}
		downloads, err := id.GetDownloads()

		if err != nil {
		    t.Fatalf("Failed to Get Downloads: %s", err.Error())
		}

		//a.Equal(len(downloads), 1, "Should Find One Download")
		a.Equal(downloads[0].ID, "ark:99999/testdownload", "Correctly Parses GUID from JSON")
		a.Equal(downloads[0].ContentURL, "s3a://bucket/key", "Correctly Retrieves content URI")
		*/

	})

}
