package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	language "cloud.google.com/go/language/apiv1"
	"cloud.google.com/go/storage"
	"github.com/SADA-U-Session-3/sentiment-analysis"
)

func main() {
	// download reddit posts from json on cloud storage

	// run posts through entity/sentiment api while abiding
	// by NL api 600 requests per minute

	// send results to cloud storage

	// signal we are finished once we have saved posts to cloud storage

	http.HandleFunc("/api/analyze/posts", analyzePostHandler)

	port := os.Getenv("PORT")

	if port == "" {
		port = "3000"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func appendToFilename(filename string, addendum string) string {
	extension := filepath.Ext(filename)

	return strings.Replace(filename, extension, "_", 1) + addendum + extension
}

func analyzePostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("must be GET request"))

		return
	}

	projectBucket := "rube_goldberg_project"
	subBucket := "reddit_data"

	query := r.URL.Query()

	// this file must live within cloud storage
	filename := query.Get("filename")
	outputFilename := appendToFilename(filename, "analyzed")

	if filename == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing required input filename"))

		return
	}

	ctx := context.Background()

	languageClient, err := language.NewClient(ctx)

	if err != nil {
		log.Printf("failed to create language client: %v\n", err)

		return
	}

	storageClient, err := storage.NewClient(ctx)

	if err != nil {
		log.Printf("failed to create storage client: %v\n", err)

		return
	}

	defer (func(languageClient *language.Client, storageClient *storage.Client) {
		languageClient.Close()
		storageClient.Close()
	})(languageClient, storageClient)

	// pull posts from cloud storage
	storageCTX, storageCTXCancel := context.WithTimeout(ctx, time.Second*50)

	defer storageCTXCancel()

	var posts []sentiment.RedditPost

	storageReader, err := storageClient.Bucket(projectBucket + "/" + subBucket).Object(filename).NewReader(storageCTX)

	if err != nil {
		log.Printf("getting bucket reader failed: %v\n", err)

		return
	}

	defer storageReader.Close()

	if err := json.NewDecoder(storageReader).Decode(&posts); err != nil {
		log.Printf("parsing json failed: %v\n", err)

		return
	}

	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "analyzing %d posts", len(posts))
	log.Printf("analyzing %d posts\n", len(posts))

	// w.Write([]byte(fmt.Sprintf("go to /api/progress?id=%d to check on this requests progress", 123)))

	analyzedPosts, err := sentiment.AnalyzeEntitesInPosts(ctx, languageClient, posts)

	if err != nil {
		log.Printf("analysis failed: %v\n", err)

		return
	}

	log.Printf("after pruning posts with empty body we analyzed %d posts\n", len(analyzedPosts))

	// save to cloud storage
	storageCTX, storageCTXCancel = context.WithTimeout(ctx, time.Second*50)

	defer storageCTXCancel()

	storageWriter := storageClient.Bucket(projectBucket).Object(subBucket + "/" + outputFilename).NewWriter(storageCTX)

	defer storageWriter.Close()

	if err := json.NewEncoder(storageWriter).Encode(analyzedPosts); err != nil {
		log.Printf("failed to upload analyzed posts: %v\n", err)

		return
	}

	log.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)

	// go signalFinished()
}
