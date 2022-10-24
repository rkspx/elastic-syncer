package esutil

import (
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type SearchResponse struct {
	Hits SearchHits `json:"hits"`
}

type SearchHits struct {
	Total struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	Hits []Document `json:"hits"`
}

func ParseSearch(res *esapi.Response) ([]Document, error) {
	defer res.Body.Close()
	if res.IsError() {
		return nil, ParseCommonError(res.Body)
	}

	var response SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}

	return response.Hits.Hits, nil
}

type SearchMetadata struct {
	Results []Document
	Total   int
}

func ParseSearchWithMetadata(res *esapi.Response) (SearchMetadata, error) {
	defer res.Body.Close()
	if res.IsError() {
		return SearchMetadata{}, ParseCommonError(res.Body)
	}

	var response SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return SearchMetadata{}, err
	}

	meta := SearchMetadata{
		Total:   response.Hits.Total.Value,
		Results: response.Hits.Hits,
	}

	return meta, nil
}
