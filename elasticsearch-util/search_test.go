package esutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func TestParseSearchResponse(t *testing.T) {
	for _, c := range []struct {
		b      []byte
		status int
		count  int
		err    error
		verify func(t *testing.T, docs []Document)
	}{
		{
			b: []byte(`{
				"took": 1337,
				"timed_out": false,
				"_shards": {
					"total": 99,
					"successful": 99,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 10000,
						"relation": "gte"
					},
					"max_score": 1.0,
					"hits": [						
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-1",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-2",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-3",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-4",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-5",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-6",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-7",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-8",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-9",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-10",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						}
					]
				}
			}`),
			count:  10,
			status: 200,
			verify: func(t *testing.T, docs []Document) {
				type FooBarQux struct {
					Foo string `json:"foo"`
				}

				for _, doc := range docs {
					var foobarqux FooBarQux
					if err := doc.Parse(&foobarqux); err != nil {
						t.Fatal(err)
					}

					if foobarqux.Foo != "bar" {
						t.Errorf("expecting foo == 'bar', got '%s'", foobarqux.Foo)
					}
				}
			},
		},
		{
			b: []byte(`
			{
				"error": {
					"root_cause": [
						{
							"type": "security_exception",
							"reason": "some security exception reason"
						}
					],
					"type": "security_exception",
					"reason": "some security exception reason"
				},
				"status": 403
			}`),
			status: 403,
			err: CommonErrorResponse{
				Err: CommonError{
					Type:   "security_exception",
					Reason: "some security exception reason",
				},
				Status: 403,
			},
			count: 0,
		},
	} {
		t.Run("test parse search response", func(t *testing.T) {
			res := &esapi.Response{
				StatusCode: c.status,
				Body:       io.NopCloser(bytes.NewReader(c.b)),
			}

			docs, err := ParseSearch(res)
			if err != c.err {
				t.Errorf("expecting error %T, got %T", c.err, err)
			}

			if err != nil && c.err != nil {
				if err.Error() != c.err.Error() {
					t.Errorf("expecting error message '%s', got '%s'", c.err.Error(), err.Error())
				}
			}

			if len(docs) != c.count {
				t.Errorf("expecting %d documents, got %d", c.count, len(docs))
			}

			if c.verify != nil {
				c.verify(t, docs)
			}
		})
	}
}

func TestParseSearchResponseWithCustomData(t *testing.T) {
	for _, c := range []struct {
		b      []byte
		status int
		count  int
		total  int
		err    error
		verify func(*testing.T, SearchMetadata)
	}{
		{
			b: []byte(`{
				"took": 1337,
				"timed_out": false,
				"_shards": {
					"total": 99,
					"successful": 99,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 10000,
						"relation": "gte"
					},
					"max_score": 1.0,
					"hits": [						
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-1",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-2",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-3",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-4",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-5",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-6",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-7",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-8",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-9",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						},
						{
							"_index": "foo_bar_qux",
							"_type": "_doc",
							"_id": "some-foo-10",
							"_score": 1.0,
							"_source": {
								"foo": "bar"
							}
						}
					]
				}
			}`),
			count:  10,
			status: 200,
			total:  10000,
			verify: func(t *testing.T, meta SearchMetadata) {
				type FooBarQux struct {
					Foo string `json:"foo"`
				}

				for _, doc := range meta.Results {
					var foobarqux FooBarQux
					if err := doc.Parse(&foobarqux); err != nil {
						t.Fatal(err)
					}

					if foobarqux.Foo != "bar" {
						t.Errorf("expecting foo == 'bar', got '%s'", foobarqux.Foo)
					}
				}
			},
		},
		{
			b: []byte(`
			{
				"error": {
					"root_cause": [
						{
							"type": "security_exception",
							"reason": "some security exception reason"
						}
					],
					"type": "security_exception",
					"reason": "some security exception reason"
				},
				"status": 403
			}`),
			status: 403,
			err: CommonErrorResponse{
				Err: CommonError{
					Type:   "security_exception",
					Reason: "some security exception reason",
				},
				Status: 403,
			},
			count: 0,
			total: 0,
		},
	} {
		t.Run("test parse search response with metadata", func(t *testing.T) {
			res := &esapi.Response{
				StatusCode: c.status,
				Body:       io.NopCloser(bytes.NewReader(c.b)),
			}

			meta, err := ParseSearchWithMetadata(res)
			if err != c.err {
				t.Errorf("expecting error %T, got %T", c.err, err)
			}

			if c.err != nil && err != nil {
				if c.err.Error() != err.Error() {
					t.Errorf("expecting error message '%s', got '%s'", c.err.Error(), err.Error())
				}
			}

			if meta.Total != c.total {
				t.Errorf("expecting total %d, got %d", c.total, meta.Total)
			}

			if len(meta.Results) != c.count {
				t.Errorf("expecting count %d, got %d", c.count, len(meta.Results))
			}

			if c.verify != nil {
				c.verify(t, meta)
			}

		})
	}
}
