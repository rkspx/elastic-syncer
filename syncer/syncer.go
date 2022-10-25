package syncer

import (
	"context"
	"fmt"
	"log"
	"time"

	util "github.com/rkspx/elastic-syncer/elasticsearch-util"
)

const (
	DefaultSince = 30 * 24 * time.Hour // 30 days
	DefaultLimit = 0
	DefaultIndex = ""
)

type Config struct {
	Since time.Duration
	Limit int
	Index string

	FromHost         string
	FromUsername     string
	FromPassword     string
	LogFromRequests  bool
	LogFromResponses bool

	ToHost         string
	ToUsername     string
	ToPassword     string
	LogToRequests  bool
	LogToResponses bool
}

type Client struct {
	fromClient *readClient
	toClient   *readWriteClient
	index      string

	from  time.Time
	to    time.Time
	limit int
}

func New(cfg Config) (*Client, error) {
	fromClient, err := newReadClient(readClientConfig{
		address:      cfg.FromHost,
		username:     cfg.FromUsername,
		password:     cfg.FromPassword,
		logRequests:  cfg.LogFromRequests,
		logResponses: cfg.LogFromResponses,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create from client, %s", err.Error())
	}

	toClient, err := newReadWriteClient(readWriteClientConfig{
		host:         cfg.ToHost,
		username:     cfg.ToUsername,
		password:     cfg.ToPassword,
		logRequests:  cfg.LogToRequests,
		logResponses: cfg.LogToResponses,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create to client, %s", err.Error())
	}

	now := time.Now().UTC()
	from, to := now.Add(-cfg.Since), now

	cl := &Client{
		fromClient: fromClient,
		toClient:   toClient,
		index:      cfg.Index,
		from:       from,
		to:         to,
		limit:      cfg.Limit,
	}

	return cl, nil
}

func (c *Client) Sync(ctx context.Context) error {
	log.Printf("syncing from '%s' to '%s'\n", c.from.Format(time.RFC3339), c.to.Format(time.RFC3339))

	go func() {
		<-ctx.Done()
		log.Println("context cancelled")
		flushContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.toClient.Flush(flushContext); err != nil {
			log.Printf("failed to flush: %s\n", err)
		}
	}()

	log.Printf("reading index settings for '%s'\n", c.index)
	settings, err := c.fromClient.ReadIndexSettings(ctx, c.index)
	if err != nil {
		return fmt.Errorf("can not get index settings for '%s', %s", c.index, err.Error())
	}

	log.Printf("found %d indexes \n", len(settings))
	for _, setting := range settings {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		log.Printf("checking index '%s' on destination elasticsearch\n", setting.Index)
		exist, err := c.toClient.IndexExist(ctx, setting.Index)
		if err != nil {
			return fmt.Errorf("can not check index exist for '%s', %s", setting.Index, err.Error())
		}

		if exist {
			log.Printf("index '%s' exist on destination elasticsearch\n", setting.Index)
			continue
		}

		log.Printf("index '%s' doesn't exist on destination elasticsearch, creating...\n", setting.Index)
		if err := c.toClient.CreateIndex(ctx, setting); err != nil {
			return fmt.Errorf("failed to create index '%s', %s", setting.Index, err.Error())
		}

		log.Printf("index '%s' created on destination elasticsearch\n", setting.Index)
	}

	req := readAllRequest{
		from:  c.from,
		to:    c.to,
		limit: c.limit,
		index: c.index,
	}

	err = c.fromClient.ReadAll(ctx, req, func(doc util.Document) {
		log.Printf("found document '%s/%s'\n", doc.Index, doc.ID)
		if err := c.toClient.WriteDocument(
			ctx,
			doc,
			func(doc util.DocumentMetadata) {
				log.Printf("done writing document '%s/%s'\n", doc.Index, doc.ID)
			},
			func(doc util.DocumentMetadata, err error) {
				log.Printf("failed to write document '%s/%s', %s\n", doc.Index, doc.ID, err.Error())
			},
		); err != nil {
			log.Printf("failed to write document '%s/%s', %s\n", doc.Index, doc.ID, err.Error())
		}
	})

	if err != nil {
		return fmt.Errorf("can not read, %s", err.Error())
	}

	c.toClient.Wait()
	return nil
}
