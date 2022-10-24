package syncer

import (
	"context"
	"io"
	"net/http"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type readClient struct {
	cl *elasticsearch.Client
}

func (r *readClient) ReadIndexSettings(ctx context.Context, index string) ([]IndexSetting, error) {
	res, err := r.cl.Indices.Get([]string{index}, r.cl.Indices.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	settings, err := parseGetIndexSettings(res)
	if err != nil {
		return nil, err
	}

	return settings, nil
}

func parseGetIndexSettings(res *esapi.Response) ([]IndexSetting, error) {
	// TODO: add implementation, parses the *esapi.Response, defer closing its body,
	// check wether the status code indicates an error, if it is, parse the error, and return
	// it as an `error`. Otherwise, parse the response into []IndexSetting, and return it
	// with nil error.
	panic("not implemented")
}

func (r *readClient) ReadAll(ctx context.Context, index string, onRead func(doc Document)) error {
	// TODO: add implementation
	// check if the index mappings contain `timestamp` field with datetime field type.
	//
	// If it does, create a point-in-time for the index, and do match_all query with sort
	// on field `timestamp` and `_id` descendingly, passing the point-in-time id along the query.
	// it then do a search_after query, passing the point-in-time id, the last result's timestamp and _id
	// as search_after parameter, and use the same sort parameter, until there's no result
	// returned from the query.

	// TODO: add result limit parameter, and/or from-to time limit.

	// If it doesn't, do a simple pagination using from and to search parameter, note that
	// the result could be inconsistent if a referesh happen during the pagination query.
	panic("not implemented")
}

type readWriteClient struct {
	cl *elasticsearch.Client
}

func (c *readWriteClient) IndexExist(ctx context.Context, index string) (bool, error) {
	res, err := c.cl.Indices.Exists([]string{index}, c.cl.Indices.Exists.WithContext(ctx))
	if err != nil {
		return false, err
	}

	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.IsError() {
		if res.StatusCode == http.StatusNotFound {
			return false, nil
		}

		return false, parseError(res.Body)
	}

	return true, nil
}

func (c *readWriteClient) CreateIndex(ctx context.Context, setting IndexSetting) error {
	// TODO: add implementation
	panic("not implemented")
}

func (c *readWriteClient) WriteDocument(ctx context.Context, index, id string, source []byte, onSuccess func(Document), onError func(Document, error)) {
	// TODO: add implementation
	panic("not implemented")
}

func (c *readWriteClient) Wait() {
	// TODO: add implementation
	panic("not implemented")
}

func parseError(res io.Reader) error {
	// TODO: add implementation
	// it's not sure how this will be implemented yet, but
	// the main goal is to return something with proper error
	// message depending on the error response.
	panic("not implemented")
}
