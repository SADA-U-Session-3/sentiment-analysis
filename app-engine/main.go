package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	language "cloud.google.com/go/language/apiv1"
	"cloud.google.com/go/storage"
	"github.com/SADA-U-Session-3/sentiment-analysis"
)

func main() {
	// listen to rube_goldberg pubsub topic

	// listen for event type and filename to parse

	// download reddit posts from json on cloud storage

	// run posts through entity/sentiment api while abiding
	// by NL api 600 requests per minute

	// send results to cloud storage

	// signal we are finished once we save posts to cloud storage

	projectBucket := "rube_goldberg_project"
	outputFilename := "reddit_data"

	ctx := context.Background()

	languageClient, err := language.NewClient(ctx)

	if err != nil {
		fmt.Printf("failed to create language client: %v\n", err)

		os.Exit(1)
	}

	storageClient, err := storage.NewClient(ctx)

	if err != nil {
		fmt.Printf("failed to create storage client: %v\n", err)

		os.Exit(1)
	}

	defer (func(languageClient *language.Client, storageClient *storage.Client) {
		languageClient.Close()
		storageClient.Close()
	})(languageClient, storageClient)
	// return

	sampleComment := []sentiment.RedditPost{
		{
			ID: "o74ykd",
			Body: `We have traditional CRUD app with frontend and backend parts, written in React and Express (NodeJS), respectively. We serve both of them on Google App Engine as two separate services. The issue is that we get unbelievably high monthly costs for serving frontend app on GAE, whereas $0 for serving backend app. I am totally frustrated because serving a standard React app (which is built to static app) on alternative platforms (e.g. Netlify) is free. Is there any way to completely disable scaling stuff and run this app on GAE for free?

			P.S. I am sorry if it is a dumb question but I couldn't figure it out on my own.
			`,
		},
	}

	fmt.Println("analyzing posts")

	analyzedPosts, err := sentiment.AnalyzeEntitesInPosts(ctx, languageClient, sampleComment)

	if err != nil {
		fmt.Printf("analysis failed: %v\n", err)

		os.Exit(1)
	}

	// save to cloud storage
	storageCTX, storageCTXCancel := context.WithTimeout(ctx, time.Second*50)

	defer storageCTXCancel()

	storageWriter := storageClient.Bucket(projectBucket).Object(outputFilename).NewWriter(storageCTX)

	defer storageWriter.Close()

	if err := json.NewEncoder(storageWriter).Encode(analyzedPosts); err != nil {
		fmt.Printf("failed to upload analyzed posts: %v\n", err)

		os.Exit(1)
	}

	fmt.Printf("uploaded analyzed posts to '%s'\n", projectBucket+"/"+outputFilename)
}

// func analyzePostHandler(w http.ResponseWriter, r *http.Request) {

// }
