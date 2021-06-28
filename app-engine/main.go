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

// AnalysisWrapper allows the analysis to be written to json without a lot of nesting
type AnalysisWrapper struct {
	ID        string           `json:"id"`
	Entity    map[string]int   `json:"entity"`
	Sentiment SentimentWrapper `json:"sentiment"`
}

// SentimentWrapper is a wrapper for a better output when writing to json
type SentimentWrapper struct {
	Score           float32 `json:"score,omitempty"`
	ParsedSentiment string  `json:"parsedSentiment"`
}

func toWrapper(posts []sentiment.RedditPost) []AnalysisWrapper {
	postsWrapper := make([]AnalysisWrapper, 0)

	for i := 0; i < len(posts); i++ {
		post := posts[i]

		wrappedPost := AnalysisWrapper{
			ID:     post.ID,
			Entity: post.Analysis.Entity.Count,
			Sentiment: SentimentWrapper{
				Score:           post.Analysis.Sentiment.Score,
				ParsedSentiment: post.Analysis.Sentiment.ParsedSentiment,
			},
		}

		postsWrapper = append(postsWrapper, wrappedPost)
	}

	return postsWrapper
}

func appendToFilename(filename string, addendum string) string {
	extension := filepath.Ext(filename)

	return strings.Replace(filename, extension, "_", 1) + addendum + extension
}

func startAnalysis(filename, outputFilename string) {

	projectBucket := "rube_goldberg_project"
	subBucket := "reddit_data"

	// initialize Google API clients
	ctx := context.Background()

	languageClient, err := language.NewClient(ctx)

	if err != nil {
		log.Printf("failed to create language client: %v\n", err)

		return
	}

	defer languageClient.Close()

	storageClient, err := storage.NewClient(ctx)

	if err != nil {
		log.Printf("failed to create storage client: %v\n", err)

		return
	}

	defer storageClient.Close()

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

	log.Printf("analyzing %d posts\n", len(posts))

	analyzedPosts, err := sentiment.AnalyzeEntitesInPosts(ctx, languageClient, posts)

	if err != nil {
		log.Printf("analysis failed: %v\n", err)

		return
	}

	log.Printf("after pruning posts with empty body we analyzed %d posts\n", len(analyzedPosts))

	wrappedPosts := toWrapper(analyzedPosts)

	// save to cloud storage
	storageCTX, storageCTXCancel = context.WithTimeout(ctx, time.Second*50)

	defer storageCTXCancel()

	storageWriter := storageClient.Bucket(projectBucket).Object(subBucket + "/" + outputFilename).NewWriter(storageCTX)

	defer storageWriter.Close()

	if err := json.NewEncoder(storageWriter).Encode(wrappedPosts); err != nil {
		log.Printf("failed to upload analyzed posts: %v\n", err)

		return
	}

	log.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)

	// go signalFinished()
}

func analyzePostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("must be GET request"))

		return
	}

	query := r.URL.Query()

	// this file must live within cloud storage
	filename := query.Get("filename")
	outputFilename := appendToFilename(filename, "analyzed")

	if filename == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing required input filename"))

		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "analyzing \"%s\"", filename)

	go startAnalysis(filename, outputFilename)
}
