package main

import (
	"github.com/gorilla/mux"
	"net/http"
	"log"
	"github.com/ClarkLabUVA/transfer/internal/server"
)

func main() {

	r := mux.NewRouter().StrictSlash(false)

	r.HandleFunc("/{prefix}/{suffix}", server.DownloadHandler).Methods("GET")
	r.HandleFunc("/{prefix}/{suffix}", server.UpdateHandler).Methods("PUT")
	r.HandleFunc("/{prefix}", server.UploadHandler).Methods("POST")


	log.Fatal(http.ListenAndServe(":8080", r))

}
