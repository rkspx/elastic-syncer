package esutil

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type CreateIndexResponse struct {
	Acknowledged bool   `json:"acknowledged"`
	Index        string `json:"index"`
}

type CreateIndexErrorResponse struct {
	Err    CreateIndexError `json:"error"`
	Status int              `json:"status"`
}

type CreateIndexError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	Index  string `json:"index"`
}

func (e CreateIndexErrorResponse) Error() string {
	return fmt.Sprintf("%d: %s - %s (%s)", e.Status, e.Err.Type, e.Err.Reason, e.Err.Index)
}

func ParseCreateIndexError(r io.Reader) error {
	var resp CreateIndexErrorResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return err
	}

	return resp
}

func ParseCreateIndexResponse(res *esapi.Response) (CreateIndexResponse, error) {
	defer res.Body.Close()
	if res.IsError() {
		return CreateIndexResponse{}, ParseCreateIndexError(res.Body)
	}

	var resp CreateIndexResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return CreateIndexResponse{}, err
	}

	return resp, nil
}
