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
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/SADA-U-Session-3/sentiment-analysis"
)

const projectID = "sada-u-sess-3-firestore"
const projectBucket = "rube_goldberg_project"
const redditBucket = "reddit_data"
const customerBucket = "customer_data"
const pubsubTopic = "rube_goldberg"

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

	pubsubClient, err := pubsub.NewClient(ctx, projectID)

	if err != nil {
		log.Printf("failed to create storage client: %v\n", err)

		return
	}

	app.ctx = ctx
	app.languageClient = languageClient
	app.storageClient = storageClient
	app.pubsubClient = pubsubClient

	// check that our topic exists, so we can function like expected
	topic := app.pubsubClient.Topic(pubsubTopic)

	doesTopicExist, err := topic.Exists(ctx)

	if err != nil {
		log.Fatalf("checking if the pubsub topic exists failed: %v", err)

		return
	}

	if !doesTopicExist {
		log.Fatalf("\"%s\" does not exist as a topic", pubsubTopic)

		return
	}

	app.pubsubTopic = topic

	defer app.closeClients()

	http.HandleFunc("/api/analyze/sentiment", analyzeSentimentHandler)
	http.HandleFunc("/api/analyze/entity", analyzeEntityHandler)

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
	ID        string                     `json:"id"`
	Entity    []sentiment.EntityWrapper  `json:"entity"`
	Sentiment sentiment.SentimentWrapper `json:"sentiment"`
}

func fromWrapper(posts []AnalysisWrapper) []sentiment.RedditPost {
	postsWrapper := make([]sentiment.RedditPost, 0)

	for i := 0; i < len(posts); i++ {
		post := posts[i]

		wrappedPost := sentiment.RedditPost{
			ID: post.ID,
		}

		postsWrapper = append(postsWrapper, wrappedPost)
	}

	return postsWrapper
}

func toWrapper(posts []sentiment.RedditPost) []AnalysisWrapper {
	postsWrapper := make([]AnalysisWrapper, 0)

	for i := 0; i < len(posts); i++ {
		post := posts[i]

		wrappedPost := AnalysisWrapper{
			ID:        post.ID,
			Entity:    post.Analysis.Entity,
			Sentiment: post.Analysis.Sentiment,
		}

		postsWrapper = append(postsWrapper, wrappedPost)
	}

	return postsWrapper
}

func addSentimentToWrapper(posts []sentiment.RedditPost, wrapperPosts []AnalysisWrapper) []AnalysisWrapper {
	for i := 0; i < len(posts); i++ {
		post := posts[i]
		for j := 0; j < len(wrapperPosts); j++ {
			wrappedPost := wrapperPosts[j]

			if post.ID == wrappedPost.ID {
				wrapperPosts[i].Sentiment = post.Analysis.Sentiment
			}
		}
	}

	return wrapperPosts
}

func addEntityToWrapper(posts []sentiment.RedditPost, wrapperPosts []AnalysisWrapper) []AnalysisWrapper {
	for i := 0; i < len(posts); i++ {
		post := posts[i]
		for j := 0; j < len(wrapperPosts); j++ {
			wrappedPost := wrapperPosts[j]

			if post.ID == wrappedPost.ID {
				wrapperPosts[i].Entity = post.Analysis.Entity
			}
		}
	}

	return wrapperPosts
}

func appendToFilename(filename string, addendum string) string {
	extension := filepath.Ext(filename)

	return strings.Replace(filename, extension, "_", 1) + addendum + extension
}

type PubSubEvent struct {
	EventType string `json:"eventType"`
	Payload   string `json:"payload"`
}

type appWrapper struct {
	ctx            context.Context
	languageClient *language.Client
	storageClient  *storage.Client
	pubsubClient   *pubsub.Client
	pubsubTopic    *pubsub.Topic
}

func (wrapper appWrapper) fetchRedditPosts(filename string) ([]sentiment.RedditPost, error) {
	storageCTX, storageCTXCancel := context.WithTimeout(wrapper.ctx, time.Second*50)

	defer storageCTXCancel()

	var posts []sentiment.RedditPost

	storageReader, err := wrapper.storageClient.Bucket(projectBucket + "/" + redditBucket).Object(filename).NewReader(storageCTX)

	if err != nil {

		return posts, fmt.Errorf("getting bucket reader failed: %v", err)
	}

	defer storageReader.Close()

	if err := json.NewDecoder(storageReader).Decode(&posts); err != nil {
		return posts, fmt.Errorf("parsing json failed: %v", err)
	}

	return posts, nil
}

func (wrapper appWrapper) fetchRedditAnalyzedPosts(filename string) ([]AnalysisWrapper, error) {
	storageCTX, storageCTXCancel := context.WithTimeout(wrapper.ctx, time.Second*50)

	defer storageCTXCancel()

	var posts []AnalysisWrapper

	storageReader, err := wrapper.storageClient.Bucket(projectBucket + "/" + redditBucket).Object(filename).NewReader(storageCTX)

	if err != nil {

		return posts, fmt.Errorf("getting bucket reader failed: %v", err)
	}

	defer storageReader.Close()

	if err := json.NewDecoder(storageReader).Decode(&posts); err != nil {
		return posts, fmt.Errorf("parsing json failed: %v", err)
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

func (wrapper appWrapper) triggerSentimentViaPubSub(filename string) error {
	if filename == "" {
		return fmt.Errorf("filename is required")
	}

	event := PubSubEvent{
		EventType: "update-post-sentiment",
		Payload:   filename,
	}

	eventBytes, err := json.Marshal(&event)

	if err != nil {
		return err
	}

	msg := &pubsub.Message{
		Data: eventBytes,
	}

	_, err = wrapper.pubsubTopic.Publish(wrapper.ctx, msg).Get(wrapper.ctx)

	return err
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

	if err := wrapper.pubsubClient.Close(); err != nil {
		log.Printf("failed to close pubsub client: %v\n", err)

		return
	}
}

func isAnalysisFilename(filename string) bool {
	filename = strings.ToLower(filename)

	return strings.Contains(filename, "analyzed")
}

// startEntityAnalysis analyzes entities from json file in google cloud storage
func startEntityAnalysis(filename string, outputFilename string, onAnalyzed func(analyzedFilename string)) {
	var wrappedPosts []AnalysisWrapper
	var posts []sentiment.RedditPost
	var postCount int
	var err error

	if isAnalysisFilename(filename) {
		log.Printf("downloading \"%s\"...\n", filename)

		wrappedPosts, err = app.fetchRedditAnalyzedPosts(filename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", filename, err)

			return
		}

		postCount = len(wrappedPosts)

		if postCount == 0 {
			log.Println("found 0 analyzed posts - Aborting...")

			return
		}

		log.Printf("found %d analyzed posts\n", postCount)

		// pull posts from cloud storage
		originalFilename := strings.Replace(filename, "_analyzed", "", 1)

		log.Printf("downloading \"%s\"...", originalFilename)

		posts, err = app.fetchRedditPosts(originalFilename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", originalFilename, err)

			return
		}

		postCount = len(posts)

		if postCount == 0 {
			log.Println("found 0 posts - Aborting...")

			return
		}

		log.Printf("starting entity analysis with %d posts\n", postCount)

		analyzedPosts, err := app.analyzeEntitySentiment(posts)

		if err != nil {
			log.Printf("failed to analyze entities from \"%s\": %v\n", filename, err)

			return
		}

		postCount = len(analyzedPosts)

		if postCount == 0 {
			log.Println("analyzed 0 posts - Aborting...")

			return
		}

		wrappedPosts = addEntityToWrapper(analyzedPosts, wrappedPosts)
	} else {
		// pull posts from cloud storage
		log.Printf("downloading \"%s\"...", filename)

		posts, err = app.fetchRedditPosts(filename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", filename, err)

			return
		}

		postCount = len(posts)

		if postCount == 0 {
			log.Println("found 0 posts - Aborting...")

			return
		}

		log.Printf("starting entity analysis with %d posts\n", postCount)

		analyzedPosts, err := app.analyzeEntitySentiment(posts)

		if err != nil {
			log.Printf("failed to analyze entities from \"%s\": %v\n", filename, err)

			return
		}

		wrappedPosts = toWrapper(analyzedPosts)
	}

	log.Printf("after pruning posts with empty body we analyzed entity on %d posts\n", postCount)

	// save to cloud storage
	if isAnalysisFilename(filename) {
		outputFilename = filename
	}

	if err := app.saveAnalyzedPosts(outputFilename, wrappedPosts); err != nil {
		log.Printf("failed to upload analyzed posts: %v\n", err)

		return
	}

	log.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)

	onAnalyzed(outputFilename)
}

// startSentimentAnalysis analyzes entities from json file in google cloud storage
func startSentimentAnalysis(filename string, outputFilename string, onAnalyzed func(analyzedFilename string)) {
	var wrappedPosts []AnalysisWrapper
	var posts []sentiment.RedditPost
	var postCount int
	var err error

	if isAnalysisFilename(filename) {
		log.Printf("downloading \"%s\"...\n", filename)

		wrappedPosts, err = app.fetchRedditAnalyzedPosts(filename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", filename, err)

			return
		}

		postCount = len(wrappedPosts)

		if postCount == 0 {
			log.Println("found 0 analyzed posts - Aborting...")

			return
		}

		log.Printf("found %d analyzed posts\n", postCount)

		// pull posts from cloud storage
		originalFilename := strings.Replace(filename, "_analyzed", "", 1)

		log.Printf("downloading \"%s\"...", originalFilename)

		posts, err = app.fetchRedditPosts(originalFilename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", originalFilename, err)

			return
		}

		postCount = len(posts)

		if postCount == 0 {
			log.Println("found 0 posts - Aborting...")

			return
		}

		log.Printf("starting sentiment analysis with %d posts\n", postCount)

		analyzedPosts, err := app.analyzeSentiment(posts)

		if err != nil {
			log.Printf("failed to analyze sentiment from \"%s\": %v\n", filename, err)

			return
		}

		postCount = len(analyzedPosts)

		if postCount == 0 {
			log.Println("analyzed 0 posts - Aborting...")

			return
		}

		wrappedPosts = addSentimentToWrapper(analyzedPosts, wrappedPosts)
	} else {
		// pull posts from cloud storage
		log.Printf("downloading \"%s\"...", filename)

		posts, err = app.fetchRedditPosts(filename)

		if err != nil {
			log.Printf("failed to fetch reddit posts from \"%s\": %v", filename, err)

			return
		}

		postCount = len(posts)

		if postCount == 0 {
			log.Println("found 0 posts - Aborting...")

			return
		}

		log.Printf("starting sentiment analysis with %d posts\n", postCount)

		analyzedPosts, err := app.analyzeSentiment(posts)

		if err != nil {
			log.Printf("failed to analyze sentiment from \"%s\": %v\n", filename, err)

			return
		}

		wrappedPosts = toWrapper(analyzedPosts)
	}

	log.Printf("after pruning posts with empty body we analyzed sentiment on %d posts\n", postCount)

	// save to cloud storage
	if isAnalysisFilename(filename) {
		outputFilename = filename
	}

	if err := app.saveAnalyzedPosts(outputFilename, wrappedPosts); err != nil {
		log.Printf("failed to upload analyzed posts: %v\n", err)

		return
	}

	log.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)

	onAnalyzed(outputFilename)
}

func analyzeEntityHandler(w http.ResponseWriter, r *http.Request) {
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

	onAnalyzed := func(analyzedFilename string) {
		app.triggerSentimentViaPubSub(analyzedFilename)
	}

	go startEntityAnalysis(filename, outputFilename, onAnalyzed)
}

func analyzeSentimentHandler(w http.ResponseWriter, r *http.Request) {
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

	onAnalyzed := func(analyzedFilename string) {
		log.Printf("finished analyzing sentiment!\nstarting next convolution...")
		// app.triggerNextStep()
	}

	go startSentimentAnalysis(filename, outputFilename, onAnalyzed)
}
