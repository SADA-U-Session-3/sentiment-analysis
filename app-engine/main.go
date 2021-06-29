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

const projectBucket = "rube_goldberg_project"
const redditBucket = "reddit_data"
const customerBucket = "customer_data"

var app appWrapper

func main() {
	// run posts through entity/sentiment api while abiding
	// by NL api 600 requests per minute
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

	app.ctx = ctx
	app.languageClient = languageClient
	app.storageClient = storageClient

	defer app.closeClients()

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

type appWrapper struct {
	ctx            context.Context
	languageClient *language.Client
	storageClient  *storage.Client
}

func (wrapper appWrapper) fetchRedditPosts(filename string) ([]sentiment.RedditPost, error) {
	storageCTX, storageCTXCancel := context.WithTimeout(wrapper.ctx, time.Second*50)

	defer storageCTXCancel()

	var posts []sentiment.RedditPost

	storageReader, err := wrapper.storageClient.Bucket(projectBucket + "/" + redditBucket).Object(filename).NewReader(storageCTX)

	if err != nil {

		return posts, fmt.Errorf("getting bucket reader failed: %v\n", err)
	}

	defer storageReader.Close()

	if err := json.NewDecoder(storageReader).Decode(&posts); err != nil {
		return posts, fmt.Errorf("parsing json failed: %v\n", err)
	}

	return posts, nil
}

func (wrapper appWrapper) saveAnalyzedPosts(outputFilename string, posts []AnalysisWrapper) error {
	storageCTX, storageCTXCancel := context.WithTimeout(wrapper.ctx, time.Second*50)

	defer storageCTXCancel()

	storageWriter := wrapper.storageClient.Bucket(projectBucket).Object(redditBucket + "/" + outputFilename).NewWriter(storageCTX)

	defer storageWriter.Close()

	return json.NewEncoder(storageWriter).Encode(posts)
}

func (wrapper appWrapper) analyzeEntitySentiment(posts []sentiment.RedditPost) ([]sentiment.RedditPost, error) {
	return sentiment.AnalyzeEntitesInPosts(wrapper.ctx, wrapper.languageClient, posts)
}

func (wrapper appWrapper) analyzeSentiment(posts []sentiment.RedditPost) ([]sentiment.RedditPost, error) {
	return sentiment.AnalyzePosts(wrapper.ctx, wrapper.languageClient, posts)
}

func (wrapper appWrapper) closeClients() {
	if err := wrapper.languageClient.Close(); err != nil {
		log.Printf("failed to close language client: %v\n", err)

		return
	}

	if err := wrapper.storageClient.Close(); err != nil {
		log.Printf("failed to close storage client: %v\n", err)

		return
	}
}

// startEntityAnalysis analyzes entities from json file in google cloud storage
func startEntityAnalysis(filename, outputFilename string) {
	// pull posts from cloud storage
	log.Println("downloading .json file")

	posts, err := app.fetchRedditPosts(filename)

	if err != nil {
		log.Printf("failed to fetch reddit posts from \"%s\": %v", filename, err)

		return
	}

	log.Printf("starting entity analysis with %d posts\n", len(posts))

	analyzedPosts, err := app.analyzeEntitySentiment(posts)

	if err != nil {
		log.Printf("failed to analyze entities from \"%s\": %v\n", filename, err)

		return
	}

	postCount := len(analyzedPosts)

	log.Printf("after pruning posts with empty body we analyzed entity on %d posts\n", postCount)

	analyzedPosts, err = app.analyzeSentiment(analyzedPosts)

	if err != nil {
		log.Printf("failed to analyze entities from \"%s\": %v\n", filename, err)

		return
	}

	log.Printf("after entity analysis we analyzed sentiment on %d posts\n", postCount)

	wrappedPosts := toWrapper(analyzedPosts)

	// save to cloud storage
	if err := app.saveAnalyzedPosts(outputFilename, wrappedPosts); err != nil {
		log.Printf("failed to upload analyzed posts: %v\n", err)

		return
	}

	log.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)

	// signalFinished()
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

	if filename == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing required input filename"))

		return
	}

	outputFilename := appendToFilename(filename, "analyzed")

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "analyzing \"%s\"", filename)

	go startEntityAnalysis(filename, outputFilename)
}
