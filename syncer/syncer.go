package syncer

import (
	"context"
	"fmt"
	"log"
	"time"

	util "github.com/rkspx/elastic-syncer/elasticsearch-util"
)

type Config struct {
	From  time.Time
	To    time.Time
	Limit int
	Index string

	FromHost     string
	FromUsername string
	FromPassword string

	ToHost     string
	ToUsername string
	ToPassword string
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
		address:  cfg.FromHost,
		username: cfg.FromUsername,
		password: cfg.FromPassword,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create from client, %s", err.Error())
	}

	toClient, err := newReadWriteClient(readWriteClientConfig{
		host:     cfg.ToHost,
		username: cfg.ToUsername,
		password: cfg.ToPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create to client, %s", err.Error())
	}

	cl := &Client{
		fromClient: fromClient,
		toClient:   toClient,
		index:      cfg.Index,
		from:       cfg.From,
		to:         cfg.To,
		limit:      cfg.Limit,
	}

	return cl, nil
}

func (c *Client) Sync(ctx context.Context) error {
	defer func() {
		flushContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.toClient.Flush(flushContext); err != nil {
			log.Printf("failed to flush: %s\n", err)
		}
	}()

	settings, err := c.fromClient.ReadIndexSettings(ctx, c.index)
	if err != nil {
		return fmt.Errorf("can not get index settings for '%s', %s", c.index, err.Error())
	}

	for _, setting := range settings {
		exist, err := c.toClient.IndexExist(ctx, setting.Index)
		if err != nil {
			return fmt.Errorf("can not check index exist for '%s', %s", setting.Index, err.Error())
		}

		if exist {
			continue
		}

		if err := c.toClient.CreateIndex(ctx, setting); err != nil {
			return fmt.Errorf("failed to create index '%s', %s", setting.Index, err.Error())
		}
	}

	req := readAllRequest{
		from:  c.from,
		to:    c.to,
		limit: c.limit,
	}

	err = c.fromClient.ReadAll(ctx, req, func(doc util.Document) {
		c.toClient.WriteDocument(
			ctx,
			doc,
			func(doc util.DocumentMetadata) {
				log.Printf("done writing document '%s/%s'\n", doc.Index, doc.ID)
			},
			func(doc util.DocumentMetadata, err error) {
				log.Printf("failed to write document '%s/%s', %s\n", doc.Index, doc.ID, err.Error())
			},
		)
	})

	if err != nil {
		return fmt.Errorf("can not read, %s", err.Error())
	}

	c.toClient.Wait()
	return nil
}
