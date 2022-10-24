package syncer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type Config struct {
}

type Client struct {
	fromClient *readClient
	toClient   *readWriteClient
	index      string

	bulkUtil esutil.BulkIndexer
}

func New(cfg Config) (*Client, error) {
	// TODO: add implementation
	panic("not implemented")
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
		// TODO: add request from config
	}

	err = c.fromClient.ReadAll(ctx, req, func(doc Document) {
		c.toClient.WriteDocument(
			ctx,
			doc,
			func(doc DocumentMetadata) {
				log.Printf("done writing document '%s/%s'\n", doc.Index, doc.ID)
			},
			func(doc DocumentMetadata, err error) {
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
