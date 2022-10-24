package syncer

import (
	"context"
	"fmt"
	"log"

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

	err = c.fromClient.ReadAll(ctx, c.index, func(doc Document) {
		c.toClient.WriteDocument(
			ctx,
			doc.Index,
			doc.ID,
			doc.Source,
			func(doc Document) {
				log.Printf("done writing document '%s/%s'\n", doc.Index, doc.ID)
			},
			func(doc Document, err error) {
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
