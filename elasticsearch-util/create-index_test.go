package esutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func TestParseCreateIndex(t *testing.T) {
	for _, c := range []struct {
		b      []byte
		err    error
		ack    bool
		status int
	}{
		{
			b: []byte(`{
				"error": {
					"root_cause": [
						{
							"type": "resource_already_exists_exception",
							"reason": "index [test-index/some-uuid] already exists",
							"index_uuid": "some-uuid",
							"index": "test-index"
						}
					],
					"type": "resource_already_exists_exception",
					"reason": "index [test-index/some-uuid] already exists",
					"index_uuid": "some-uuid",
					"index": "test-index"
				},
				"status": 400
			}`),
			err: CreateIndexErrorResponse{
				Status: 400,
				Err: CreateIndexError{
					Type:   "resource_already_exists_exception",
					Reason: "index [test-index/some-uuid] already exists",
					Index:  "test-index",
				},
			},
			status: 400,
		},
		{
			b: []byte(`
			{
				"acknowledged": true,
				"shards_acknowledged": true,
				"index": "test-index-2"
			}`),
			status: 200,
			ack:    true,
		},
	} {
		esres := &esapi.Response{
			StatusCode: c.status,
			Body:       io.NopCloser(bytes.NewReader(c.b)),
		}

		res, err := ParseCreateIndexResponse(esres)
		if err != c.err {
			t.Errorf("exepcting error %v, got %v", c.err, err)
		}

		if res.Acknowledged != c.ack {
			t.Errorf("expecting ack %t, got %t", c.ack, res.Acknowledged)
		}
	}
}
