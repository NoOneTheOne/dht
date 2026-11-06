package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/NoOneTheOne/dht/pkg/basic"
	"github.com/NoOneTheOne/dht/pkg/config"
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
		})
		if err != nil {
			log.Fatalf("failed to create elasticsearch client: %v", err)
		}
	})
	return client
}

func Write(i, t string, data []byte) (string, error) {
	req := esapi.CreateRequest{
		Index:        i,
		DocumentType: t,
		DocumentID:   basic.RandStr(20),
		Body:         bytes.NewReader(data),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := req.Do(ctx, getClient())
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("elasticsearch write error: status=%d index=%s", res.StatusCode, i)
	}

	return res.String(), nil
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
