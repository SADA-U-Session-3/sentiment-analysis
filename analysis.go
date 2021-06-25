package sentiment

import (
	"context"
	"fmt"

	language "cloud.google.com/go/language/apiv1"
	languagepb "google.golang.org/genproto/googleapis/cloud/language/v1"
)

// RedditPost is the struct of a reddit post pulled from this repos' scraped post
type RedditPost struct {
	Title            string   `json:"title"`
	Score            int      `json:"score"`
	ID               string   `json:"id"`
	URL              string   `json:"url"`
	CommentCount     int      `json:"comms_num"`
	CreatedAt        float32  `json:"created"`
	Body             string   `json:"body"`
	Timestamp        float32  `json:"timestamp"` // same as CreatedAt
	Comments         []string `json:"comments"`
	Analysis         Analysis `json:"analysis"`
	CommentsAnalysis Analysis `json:"commentsAnalysis"`
}

// Analysis hold the results from the sentiment analysis from Google's API
type Analysis struct {
	Sentiment struct {
		Score           float32 `json:"score"`
		ParsedSentiment string  `json:"parsedSentiment"`
	} `json:"sentiment"`
	Entity struct {
		Count map[string]int `json:"count"`
	} `json:"entity"`
}

// Posts a wrapper struct around the Hot and Top posts that help parse the scraped Reddit posts in this repo
type Posts struct {
	HotPosts []RedditPost `json:"hot_posts"`
	TopPosts []RedditPost `json:"top_posts"`
}

// PrintAnalysis prints the results of the posts from the Sentiment Analysis api
func PrintAnalysis(posts []RedditPost) {
	for i := 0; i < len(posts); i++ {
		post := posts[i]

		fmt.Printf("post id: \"%s\"\n\ttitle: \"%s\"\n\tbody: \"%s\"\n\tsentiment for post: %s\n\tsentiment score: %f\n",
			post.ID,
			post.Title,
			post.Body,
			post.Analysis.Sentiment.ParsedSentiment,
			post.Analysis.Sentiment.Score,
		)
	}
}

// func parseSentiment(score float32) string {
// 	if score == 0.0 {
// 		return "mixed"
// 	} else if score == 0.1 {
// 		return "neutral"
// 	} else if score > 0.1 {
// 		return "positive"
// 	} else if score < 0.1 && score > 0.0 {
// 		return "mixed"
// 	} else if score < 0.0 {
// 		return "negative"
// 	} else {
// 		return "unknown"
// 	}
// }

// PrintSentimentChart prints the sentiment analysis chart
func PrintSentimentChart() {
	fmt.Printf("To interpret the scores:\n\tpositive: > 0.1\n\tnegative: < 0.0\n\tneutral: 0.1\n\tmixed: 0.0 - 0.1\n")
}

// pruneEmptyPosts remove reddit posts where the submitter did not write text in the post
func pruneEmptyPosts(posts []RedditPost) []RedditPost {
	postsWithBodyText := make([]RedditPost, 0)

	for i := 0; i < len(posts); i++ {
		post := posts[i]

		if post.Body == "" {
			continue
		}

		postsWithBodyText = append(postsWithBodyText, post)
	}

	return postsWithBodyText
}

// pruneEmptyComments remove reddit posts where there is no comments
func pruneEmptyComments(posts []RedditPost) []RedditPost {
	postsWithComments := make([]RedditPost, 0)

	for i := 0; i < len(posts); i++ {
		post := posts[i]

		if post.CommentCount < 1 {
			continue
		}

		postsWithComments = append(postsWithComments, post)
	}

	return postsWithComments
}

// getEntityCount counts all instances of each entity found
func getEntityCount(entities []*languagepb.Entity) map[string]int {
	entityTracker := make(map[string]int)

	for i := 0; i < len(entities); i++ {
		entity := entities[i]

		if _, ok := entityTracker[entity.Name]; !ok {
			entityTracker[entity.Name] = 1
		} else {
			entityTracker[entity.Name]++
		}
	}

	return entityTracker
}

// func analyzeSentiment(ctx context.Context, client *language.Client, text string) (*languagepb.AnalyzeSentimentResponse, error) {
// 	return client.AnalyzeSentiment(ctx, &languagepb.AnalyzeSentimentRequest{
// 		Document: &languagepb.Document{
// 			Source: &languagepb.Document_Content{
// 				Content: text,
// 			},
// 			Type: languagepb.Document_PLAIN_TEXT,
// 		},
// 	})
// }

func analyzeEntitySentiment(ctx context.Context, client *language.Client, text string) (*languagepb.AnalyzeEntitySentimentResponse, error) {
	return client.AnalyzeEntitySentiment(ctx, &languagepb.AnalyzeEntitySentimentRequest{
		Document: &languagepb.Document{
			Source: &languagepb.Document_Content{
				Content: text,
			},
			Type: languagepb.Document_PLAIN_TEXT,
		},
	})
}

// AnalyzeEntitiesInPosts analyzes the entities in a reddit post and appends that analysis to each post
func AnalyzeEntitesInPosts(ctx context.Context, client *language.Client, posts []RedditPost) ([]RedditPost, error) {
	postsWithBodyText := pruneEmptyPosts(posts)
	postCount := len(postsWithBodyText)

	// Google's limits: 600 requests per minute, 800k per day
	// TODO: limit the requests to 10 request per second to abide to Google's limit
	for i := 0; i < postCount; i++ {
		post := postsWithBodyText[i]

		analysis, err := analyzeEntitySentiment(ctx, client, post.Body)

		if err != nil {
			return []RedditPost{}, err
		}

		post.Analysis.Entity.Count = getEntityCount(analysis.Entities)

		postsWithBodyText[i] = post
	}

	return postsWithBodyText, nil

}

// analyzePosts send each reddit post's body to Google's api for sentiment analysis
// mutates each post's Analyze.Score property and return the posts and no error
// if an error is present then empty posts and nil
func AnalyzePosts(ctx context.Context, client *language.Client, posts []RedditPost) ([]RedditPost, error) {
	postsWithBodyText := pruneEmptyPosts(posts)
	// postCount := len(postsWithBodyText)

	// Google's limits: 600 requests per minute, 800k per day
	// TODO: limit the requests to 10 request per second to abide to Google's limit
	// for i := 0; i < postCount; i++ {
	// post := postsWithBodyText[i]

	// analysis, err := analyzeSentiment(ctx, client, post.Body)

	// if err != nil {
	// 	return []RedditPost{}, err
	// }

	// score := analysis.DocumentSentiment.Score

	// Keep a running total of the sentiment
	// postsWithBodyText[i].Analysis.Sentiment.Score += score
	// postsWithBodyText[i].Analysis.Sentiment.ParsedSentiment = parseSentiment(score)
	// }

	return postsWithBodyText, nil
}

// analyzeComments send each reddit post's comment to Google's api for sentiment analysis
// mutates each post's Analyze.Score property and return the posts and no error
// if an error is present then empty posts and nil
func AnalyzeComments(ctx context.Context, client *language.Client, posts []RedditPost) ([]RedditPost, error) {
	postsWithComments := pruneEmptyComments(posts)
	// postCount := len(postsWithComments)

	// Google's limits: 600 requests per minute, 800k per day
	// TODO: limit the requests to 10 request per second to abide to Google's limit
	// for i := 0; i < postCount; i++ {
	// 	post := postsWithComments[i]

	// 	analysis, err := analyzeSentiment(ctx, client, post.Body)

	// 	if err != nil {
	// 		return []RedditPost{}, err
	// 	}

	// 	score := analysis.DocumentSentiment.Score

	// Keep a running total of the sentiment
	// 	postsWithComments[i].CommentsAnalysis.Sentiment.Score += score
	// 	postsWithComments[i].CommentsAnalysis.Sentiment.ParsedSentiment = parseSentiment(score)
	// }

	return postsWithComments, nil
}
