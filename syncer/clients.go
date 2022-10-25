package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
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
	pointInTimeKeepAlive   = "1m"
)

var (
	// ErrNoReadIndex is error returned when trying to read from elasticsearch without specifying any index name.
	ErrNoReadIndex = errors.New("no read index specified")
)

type readAllRequest struct {
	from  time.Time
	to    time.Time
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
		if r.to.IsZero() {
			r.to = now
		}

		if r.from.IsZero() {
			r.from = now.Add(-defaultReadAllInterval)
		}
	}
}

type readClientConfig struct {
	address  string
	username string
	password string
}

func (r readClientConfig) validate() error {
	if r.address == "" {
		return ErrNoHost
	}

	return nil
}

func newReadClient(cfg readClientConfig) (*readClient, error) {
	cl, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.address},
		Username:  cfg.username,
		Password:  cfg.password,
	})

	if err != nil {
		return nil, err
	}

	return &readClient{
		cl: cl,
	}, nil
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
	res, err := r.cl.OpenPointInTime([]string{index}, pointInTimeKeepAlive, r.cl.OpenPointInTime.WithContext(ctx))
	if err != nil {
		return "", err
	}

	return util.ParseOpenPIT(res)
}

func (r *readClient) closePIT(ctx context.Context, pit string) error {
	res, err := r.cl.ClosePointInTime(r.cl.ClosePointInTime.WithBody(util.PointInTime{ID: pit}))
	if err != nil {
		return err
	}

	return util.ParseClosePIT(res)
}

func (r *readClient) searchAll(ctx context.Context, req readAllRequest, pit string) ([]util.Document, error) {
	body, err := r.searchAllBody(req, pit)
	if err != nil {
		return nil, err
	}

	res, err := r.cl.Search(
		r.cl.Search.WithIndex(req.index),
		r.cl.Search.WithBody(body),
	)

	if err != nil {
		return nil, err
	}

	return util.ParseSearch(res)
}

func (r *readClient) searchAllBody(req readAllRequest, pit string) (io.Reader, error) {
	filters := []map[string]any{}
	if req.from.IsZero() && req.to.IsZero() {
		filters = append(filters, map[string]any{
			"range": map[string]any{
				"timestamp": map[string]any{
					"gte":    req.from.UnixMilli(),
					"lte":    req.to.UnixMilli(),
					"format": "epoch_millis",
				},
			},
		})
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": map[string]any{
					"match_all": map[string]string{},
				},
				"filters": filters,
			},
		},
		"pit": map[string]string{
			"id":         pit,
			"keep_alive": pointInTimeKeepAlive,
		},
		"sort": []map[string]string{
			{"timestamp": "desc"},
			{"_id": "desc"},
		},
	}

	b, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b), nil
}

func (r *readClient) searchAllAfter(ctx context.Context, req readAllRequest, pit string, last util.SortMetadata) ([]util.Document, error) {
	body, err := r.searchAllAfterBody(req, pit, last)
	if err != nil {
		return nil, err
	}

	res, err := r.cl.Search(
		r.cl.Search.WithIndex(req.index),
		r.cl.Search.WithBody(body),
	)

	if err != nil {
		return nil, err
	}

	return util.ParseSearch(res)
}

func (r *readClient) searchAllAfterBody(req readAllRequest, pit string, last util.SortMetadata) (io.Reader, error) {
	filters := []map[string]any{}
	if req.from.IsZero() && req.to.IsZero() {
		filters = append(filters, map[string]any{
			"range": map[string]any{
				"timestamp": map[string]any{
					"gte":    req.from.UnixMilli(),
					"lte":    req.to.UnixMilli(),
					"format": "epoch_millis",
				},
			},
		})
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": map[string]any{
					"match_all": map[string]string{},
				},
				"filters": filters,
			},
		},
		"pit": map[string]string{
			"id":         pit,
			"keep_alive": pointInTimeKeepAlive,
		},
		"search_after": last,
		"sort": []map[string]string{
			{"timestamp": "desc"},
			{"_id": "desc"},
		},
	}

	b, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b), nil
}

func (r *readClient) readAllPIT(ctx context.Context, req readAllRequest, onRead func(doc util.Document)) error {
	pit, err := r.createPIT(ctx, req.index)
	if err != nil {
		return fmt.Errorf("can not create point-in-time, %s", err.Error())
	}

	docs, err := r.searchAll(ctx, req, pit)
	count := 0
	for len(docs) >= 0 {
		// return early if limit is reached.
		if req.limit != 0 && count >= req.limit {
			return nil
		}

		if err != nil {
			return fmt.Errorf("can not read all, %s", err.Error())
		}

		count = count + len(docs)
		for _, doc := range docs {
			r.wg.Add(1)
			go func(doc util.Document) {
				defer r.wg.Done()
				onRead(doc)
			}(doc)
		}

		docs, err = r.searchAllAfter(ctx, req, pit, docs[len(docs)-1].SortMetadata)
	}

	return nil
}

func (r *readClient) readAllPaginate(ctx context.Context, req readAllRequest, onRead func(doc util.Document)) error {
	docs, total, err := r.searchLimitOffset(ctx, req, paginateLimit, 0)
	count, page := 0, 0
	for len(docs) >= 0 && total != 0 {
		// return early if limit is reached.
		if req.limit != 0 && count >= req.limit {
			return nil
		}

		if err != nil {
			return fmt.Errorf("can not read paginate, %s", err.Error())
		}

		count = count + len(docs)
		for _, doc := range docs {
			r.wg.Add(1)
			go func(doc util.Document) {
				defer r.wg.Done()
				onRead(doc)
			}(doc)
		}

		docs, total, err = r.searchLimitOffset(ctx, req, paginateLimit, page*paginateLimit)
	}

	return nil
}

func (r *readClient) searchLimitOffset(ctx context.Context, req readAllRequest, limit, offset int) ([]util.Document, int, error) {
	body, err := r.searchLimitOffsetBody(req, limit, offset)
	if err != nil {
		return nil, 0, err
	}

	res, err := r.cl.Search(
		r.cl.Search.WithIndex(req.index),
		r.cl.Search.WithBody(body),
	)

	if err != nil {
		return nil, 0, err
	}

	meta, err := util.ParseSearchWithMetadata(res)
	if err != nil {
		return nil, 0, err
	}

	return meta.Results, meta.Total, nil
}

func (r *readClient) searchLimitOffsetBody(req readAllRequest, limit int, offset int) (io.Reader, error) {
	filters := []map[string]any{}
	if req.from.IsZero() && req.to.IsZero() {
		filters = append(filters, map[string]any{
			"range": map[string]any{
				"timestamp": map[string]any{
					"gte":    req.from.UnixMilli(),
					"lte":    req.to.UnixMilli(),
					"format": "epoch_millis",
				},
			},
		})
	}

	query := map[string]any{
		"from": offset,
		"size": limit,
		"query": map[string]any{
			"bool": map[string]any{
				"must": map[string]any{
					"match_all": map[string]string{},
				},
				"filters": filters,
			},
		},
		"sort": []map[string]string{
			{"timestamp": "desc"},
			{"_id": "desc"},
		},
	}

	b, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b), nil
}

func (r *readClient) ReadAll(ctx context.Context, req readAllRequest, onRead func(doc util.Document)) error {
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

var ErrNoHost = errors.New("no elasticsearch host specified")

var (
	defaultWorkerNumber  = runtime.NumCPU()
	defaultFlushBytes    = 5e+6
	defaultFlushInterval = 30 * time.Second
)

type readWriteClientConfig struct {
	host          string
	username      string
	password      string
	workerNumber  int
	flushBytes    float64
	flushInterval time.Duration
}

func (rw *readWriteClientConfig) validate() error {
	if rw.host != "" {
		return ErrNoHost
	}

	return nil
}

func (rw *readWriteClientConfig) setDefaults() {
	if rw.workerNumber == 0 {
		rw.workerNumber = defaultWorkerNumber
	}

	if rw.flushBytes == 0 {
		rw.flushBytes = defaultFlushBytes
	}

	if rw.flushInterval == 0 {
		rw.flushInterval = defaultFlushInterval
	}
}

func newReadWriteClient(cfg readWriteClientConfig) (*readWriteClient, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

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
		FlushBytes:    int(cfg.flushBytes),
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

		return false, util.ParseCommonError(res.Body)
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

func (c *readWriteClient) WriteDocument(ctx context.Context, doc util.Document, onSuccess func(util.DocumentMetadata), onError func(util.DocumentMetadata, error)) {
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
