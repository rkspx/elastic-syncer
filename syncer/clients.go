package syncer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"

	util "github.com/rkspx/elastic-syncer/elasticsearch-util"
)

type readClient struct {
	cl *elasticsearch.Client
}

func (r *readClient) ReadIndexSettings(ctx context.Context, index string) ([]util.IndexSetting, error) {
	res, err := r.cl.Indices.Get([]string{index}, r.cl.Indices.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	settings, err := util.ParseIndicesGetResponse(res)
	if err != nil {
		return nil, err
	}

	return settings, nil
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

type readWriteClientConfig struct {
	host          string
	username      string
	password      string
	workerNumber  int
	flushBytes    int
	flushInterval time.Duration
}

func newReadWriteClient(cfg readWriteClientConfig) (*readWriteClient, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     []string{cfg.host},
		Username:      cfg.username,
		Password:      cfg.password,
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(attempt int) time.Duration {
			if attempt == 1 {
				retryBackoff.Reset()
			}

			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	})

	if err != nil {
		return nil, fmt.Errorf("error creating elasticsearch client, %s", err.Error())
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        es,
		NumWorkers:    cfg.workerNumber,
		FlushBytes:    cfg.flushBytes,
		FlushInterval: cfg.flushInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating indexer, %s", err.Error())
	}

	return &readWriteClient{
		cl: es,
		bi: bi,
	}, nil
}

type readWriteClient struct {
	cl *elasticsearch.Client
	bi esutil.BulkIndexer
	wg sync.WaitGroup
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

func (c *readWriteClient) CreateIndex(ctx context.Context, setting util.IndexSetting) error {
	res, err := c.cl.Indices.Create(setting.Index, c.cl.Indices.Create.WithBody(setting))
	if err != nil {
		return err
	}

	_, err = util.ParseCreateIndexResponse(res)
	if err != nil {
		return err
	}

	return nil
}

func (c *readWriteClient) WriteDocument(ctx context.Context, doc Document, onSuccess func(DocumentMetadata), onError func(DocumentMetadata, error)) {
	meta := doc.DocumentMetadata
	c.wg.Add(1)

	err := c.bi.Add(ctx, esutil.BulkIndexerItem{
		Action:     "index",
		DocumentID: doc.ID,
		Index:      doc.Index,
		Body:       doc,
		OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			defer c.wg.Done()
			onSuccess(meta)
		},
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			defer c.wg.Done()
			onError(meta, err)
		},
	})

	if err != nil {
		c.wg.Done()
		onError(meta, err)
	}
}

func (c *readWriteClient) Flush(ctx context.Context) error {
	return c.bi.Close(ctx)
}

func (c *readWriteClient) Wait() {
	c.wg.Wait()
}

func parseError(res io.Reader) error {
	// TODO: add implementation
	// it's not sure how this will be implemented yet, but
	// the main goal is to return something with proper error
	// message depending on the error response.
	panic("not implemented")
}
