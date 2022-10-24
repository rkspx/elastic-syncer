package syncer

import (
	"context"
	"errors"
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

const (
	defaultReadAllInterval = 24 * time.Hour
	paginateLimit          = 100
)

var (
	ErrNoReadIndex = errors.New("no read index specified")
)

type readAllRequest struct {
	from  int64
	to    int64
	limit int
	index string
}

func (r readAllRequest) validate() error {
	if r.index == "" {
		return ErrNoReadIndex
	}

	return nil
}

func (r *readAllRequest) setDefaults() {
	if r.limit == 0 {
		now := time.Now().UTC()
		if r.to == 0 {
			r.to = now.UnixMilli()
		}

		if r.from == 0 {
			r.from = now.Add(-defaultReadAllInterval).UnixMilli()
		}
	}
}

type readClient struct {
	cl *elasticsearch.Client
	wg sync.WaitGroup
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

func (r *readClient) hasTimestamp(ctx context.Context, index string) (bool, error) {
	settings, err := r.ReadIndexSettings(ctx, index)
	if err != nil {
		return false, err
	}

	for _, setting := range settings {
		if setting.Index == index {
			m, ok := setting.Setting.Mappings.Properties["timestamp"]
			if !ok {
				return false, nil
			}

			if m.Type == "date" {
				return true, nil
			}
		}
	}

	return false, nil
}

func (r *readClient) createPIT(ctx context.Context, index string) (string, error) {
	// TODO: add implementation
	panic("not implemented")
}

func (r *readClient) searchAll(ctx context.Context, req readAllRequest, pit string) ([]Document, error) {
	// TODO: add implementation
	panic("not implemented")
}

func (r *readClient) searchAllAfter(ctx context.Context, req readAllRequest, pit string, last SortMetadata) ([]Document, error) {
	// TODO: add implementation
	panic("not implemented")
}

func (r *readClient) readAllPIT(ctx context.Context, req readAllRequest, onRead func(doc Document)) error {
	pit, err := r.createPIT(ctx, req.index)
	if err != nil {
		return fmt.Errorf("can not create point-in-time, %s", err.Error())
	}

	docs, err := r.searchAll(ctx, req, pit)
	for len(docs) >= 0 {
		if err != nil {
			return fmt.Errorf("can not read all, %s", err.Error())
		}

		for _, doc := range docs {
			r.wg.Add(1)
			go func(doc Document) {
				defer r.wg.Done()
				onRead(doc)
			}(doc)
		}

		docs, err = r.searchAllAfter(ctx, req, pit, docs[len(docs)-1].SortMetadata)
	}

	return nil
}

func (r *readClient) readAllPaginate(ctx context.Context, req readAllRequest, onRead func(doc Document)) error {
	docs, total, err := r.searchLimitOffset(ctx, req, paginateLimit, 0)
	page := 0
	for len(docs) >= 0 && total != 0 {
		if err != nil {
			return fmt.Errorf("can not read paginate, %s", err.Error())
		}

		for _, doc := range docs {
			r.wg.Add(1)
			go func(doc Document) {
				defer r.wg.Done()
				onRead(doc)
			}(doc)
		}

		docs, total, err = r.searchLimitOffset(ctx, req, paginateLimit, page*paginateLimit)
	}

	return nil
}

func (r *readClient) searchLimitOffset(ctx context.Context, req readAllRequest, limit, offset int) ([]Document, int, error) {
	// TODO: add implementation
	panic("not implemented")
}

func (r *readClient) ReadAll(ctx context.Context, req readAllRequest, onRead func(doc Document)) error {
	req.setDefaults()
	if err := req.validate(); err != nil {
		return err
	}

	// TODO: add implementation
	// check if the index mappings contain `timestamp` field with datetime field type.

	// If it does, create a point-in-time for the index, and do match_all query with sort
	// on field `timestamp` and `_id` descendingly, passing the point-in-time id along the query.
	// it then do a search_after query, passing the point-in-time id, the last result's timestamp and _id
	// as search_after parameter, and use the same sort parameter, until there's no result
	// returned from the query.

	// TODO: add result limit parameter, and/or from-to time limit.

	// If it doesn't, do a simple pagination using from and to search parameter, note that
	// the result could be inconsistent if a referesh happen during the pagination query.

	ok, err := r.hasTimestamp(ctx, req.index)
	if err != nil {
		return err
	}

	if ok {
		return r.readAllPIT(ctx, req, onRead)
	}

	return r.readAllPaginate(ctx, req, onRead)
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
