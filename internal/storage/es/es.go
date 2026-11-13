package es

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

var (
	client     *elasticsearch.Client
	clientOnce sync.Once
)

func getClient() *elasticsearch.Client {
	clientOnce.Do(func() {
		c := config.GetConfig()
		var err error
		client, err = elasticsearch.NewClient(elasticsearch.Config{
			Addresses: c.Elasticsearch.Addresses,
			Username:  c.Elasticsearch.Username,
			Password:  c.Elasticsearch.Password,
			// A lost update response must not cause another count increment.
			DisableRetry: true,
		})
		if err != nil {
			log.Fatalf("failed to create elasticsearch client: %v", err)
		}
	})
	return client
}

// Write counts successful metadata writes, not DHT announces.
func Write(i, t string, data []byte) (string, error) {
	return write(nil, i, t, data)
}

// Injected clients must disable transport retries because increments are not idempotent.
func write(c *elasticsearch.Client, i, t string, data []byte) (string, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", fmt.Errorf("invalid metadata JSON: %w", err)
	}
	var infohash string
	if err := json.Unmarshal(doc["infohash"], &infohash); err != nil || len(infohash) != 40 {
		return "", fmt.Errorf("invalid infohash: expected 40 hexadecimal characters")
	}
	if _, err := hex.DecodeString(infohash); err != nil {
		return "", fmt.Errorf("invalid infohash: expected 40 hexadecimal characters")
	}
	infohash = strings.ToLower(infohash)
	doc["infohash"], _ = json.Marshal(infohash)
	// The script increments on creation too; start at zero regardless of caller input.
	doc["count"] = json.RawMessage("0")
	body, err := json.Marshal(map[string]interface{}{
		"scripted_upsert": true,
		"script": map[string]interface{}{
			"lang": "painless",
			"source": "if (!ctx._source.containsKey('count') || ctx._source.count == null) { " +
				"ctx._source.count = 0; } ctx._source.count += 1;",
		},
		"upsert": doc,
	})
	if err != nil {
		return "", fmt.Errorf("encode metadata update: %w", err)
	}
	retryOnConflict := 3
	req := esapi.UpdateRequest{
		Index:           i,
		DocumentType:    t,
		DocumentID:      infohash,
		Body:            bytes.NewReader(body),
		RetryOnConflict: &retryOnConflict,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if c == nil {
		c = getClient()
	}
	// Only Elasticsearch's bounded version-conflict retries are safe here.
	res, err := req.Do(ctx, c)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("elasticsearch write error: status=%d index=%s", res.StatusCode, i)
	}

	response, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("read elasticsearch write response: %w", err)
	}
	return fmt.Sprintf("[%s] %s", res.Status(), response), nil
}

func Search(q string) map[string]interface{} {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"name": q,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Printf("error encoding query: %v", err)
		return nil
	}

	c := config.GetConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := getClient().Search(
		getClient().Search.WithContext(ctx),
		getClient().Search.WithIndex(c.Elasticsearch.Index),
		getClient().Search.WithBody(&buf),
		getClient().Search.WithTrackTotalHits(true),
		getClient().Search.WithPretty(),
	)
	if err != nil {
		log.Printf("error searching elasticsearch: %v", err)
		return nil
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("elasticsearch search error: status=%d", res.StatusCode)
		return nil
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("error parsing search response: %v", err)
		return nil
	}
	return r
}
