package esutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func TestParseOpenPIT(t *testing.T) {
	for _, c := range []struct {
		b      []byte
		err    error
		id     string
		status int
	}{
		{
			b: []byte(`{
			"id": "some-id="
		  }`),
			id:     "some-id=",
			status: 200,
		},
		{
			b: []byte(`{
				"error": {
				  "root_cause": [
					{
					  "type": "index_not_found_exception",
					  "reason": "no such index [test-index-2]",
					  "resource.type": "index_or_alias",
					  "resource.id": "test-index-2",
					  "index_uuid": "_na_",
					  "index": "test-index-2"
					}
				  ],
				  "type": "index_not_found_exception",
				  "reason": "no such index [test-index-2]",
				  "resource.type": "index_or_alias",
				  "resource.id": "test-index-2",
				  "index_uuid": "_na_",
				  "index": "test-index-2"
				},
				"status": 404
			  }`),
			status: 404,
			err: CommonErrorResponse{
				Status: 404,
				Err: CommonError{
					Type:   "index_not_found_exception",
					Reason: "no such index [test-index-2]",
					Index:  "test-index-2",
				},
			},
		},
	} {
		t.Run("test-parse-pit", func(t *testing.T) {
			esres := &esapi.Response{
				StatusCode: c.status,
				Body:       io.NopCloser(bytes.NewReader(c.b)),
			}

			id, err := ParseOpenPIT(esres)
			if err != c.err {
				t.Errorf("expecting error %T, got %T", c.err, err)
			}

			if c.err != nil && err != nil {
				if c.err.Error() != err.Error() {
					t.Errorf("expecting error message '%s', got '%s'", c.err.Error(), err.Error())
				}
			}

			if id != c.id {
				t.Errorf("expecting pit ID '%s', got '%s'", c.id, id)
			}
		})
	}
}

func TestParseClosePIT(t *testing.T) {
	for _, c := range []struct {
		b      []byte
		err    error
		status int
	}{
		{
			b: []byte(`{
				"succeeded": true,
				"num_freed": 1
			  }`),
			status: 200,
		},
		{
			b: []byte(`{
				"succeeded": false,
				"num_freed": 0
			  }`),
			status: 404,
			err:    ErrPITNotFound,
		},
	} {
		esres := &esapi.Response{
			StatusCode: c.status,
			Body:       io.NopCloser(bytes.NewReader(c.b)),
		}

		err := ParseClosePIT(esres)
		if err != c.err {
			t.Errorf("expecting error %T, got %T", c.err, err)
		}

		if c.err != nil && err != nil {
			if c.err.Error() != err.Error() {
				t.Errorf("expecting error message '%s', got '%s'", c.err.Error(), err.Error())
			}
		}
	}
}

// '404: index_not_found_exception - test-index-2 (no such index [test-index-2])'
// '404:  - test-index-2 (no such index [test-index-2])'
