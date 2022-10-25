package esutil

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func TestParseIndicesGetResponse(t *testing.T) {
	for _, c := range []struct {
		b                []byte
		err              error
		numberOfShards   int
		numberOfReplicas int
		status           int
	}{
		{
			b: []byte(`{
				"test-index": {
					"aliases": {},
					"mappings": {},
					"settings": {
						"index": {
							"routing": {
								"allocation": {
									"include": {
										"_tier_preference": "data_content"
									}
								}
							},
							"number_of_shards": "1",
							"provided_name": "test-index",
							"creation_date": "some-date",
							"number_of_replicas": "1",
							"uuid": "some-uuid",
							"version": {
								"created": "some-number"
							}
						}
					}
				}
			}`),
			numberOfShards:   1,
			numberOfReplicas: 1,
			status:           200,
		},
		{
			b: []byte(`
			{
				"error": {
					"root_cause": [
						{
							"type": "index_not_found_exception",
							"reason": "no such index [test-index-3]",
							"resource.type": "index_or_alias",
							"resource.id": "test-index-3",
							"index_uuid": "_na_",
							"index": "test-index-3"
						}
					],
					"type": "index_not_found_exception",
					"reason": "no such index [test-index-3]",
					"resource.type": "index_or_alias",
					"resource.id": "test-index-3",
					"index_uuid": "_na_",
					"index": "test-index-3"
				},
				"status": 404
			}`),
			err: IndicesGetErrorResponse{
				Status: 404,
				Err: IndicesGetError{
					Type:   "index_not_found_exception",
					Reason: "no such index [test-index-3]",
					Index:  "test-index-3",
				},
			},
			status: 404,
		},
	} {
		esres := &esapi.Response{
			StatusCode: c.status,
			Body:       io.NopCloser(bytes.NewReader(c.b)),
		}

		res, err := ParseIndicesGetResponse(esres)
		if err != c.err {
			t.Errorf("expecting error %T, got %T", c.err, err)
		}

		if c.err != nil && err != nil {
			if c.err.Error() != err.Error() {
				t.Errorf("expecting error message '%s', got '%s'", c.err.Error(), err.Error())
			}
		}

		for _, r := range res {
			if r.Replicas() != c.numberOfReplicas {
				t.Errorf("expecting %d replicas, got %d", c.numberOfReplicas, r.Replicas())
			}

			if r.Shards() != c.numberOfShards {
				t.Errorf("expecting %d shards, got %d", c.numberOfShards, r.Shards())
			}
		}

	}
}

func TestIndexSettingPropertiesMappingParse(t *testing.T) {
	for _, c := range []struct {
		b    []byte
		prop Mappings
	}{
		{
			b: []byte(`{
				"properties": {
					"dst_ips": {
						"type": "text",
						"fields": {
							"keyword": {
								"type": "keyword",
								"ignore_above": 256
							}
						}
					},
				}
			}`),
			prop: Mappings{
				Properties: map[string]MappingProperty{
					"dst_ips": {
						Type: "text",
						Fields: map[string]MappingPropertyFieldType{
							"keyword": {
								Type:        "keyword",
								IgnoreAbove: 256,
							},
						},
					},
				},
			},
		},
	} {
		t.Run("test parse property mapping", func(t *testing.T) {
			var m Mappings
			if err := json.Unmarshal(c.b, &m); err != nil {
				t.Fatal(err)
			}

			compareMapping(t, c.prop, m)
		})
	}
}

func compareMapping(t *testing.T, m1, m2 Mappings) {
	if len(m1.Properties) != len(m2.Properties) {
		t.Errorf("expecting %d properties, got %d", len(m1.Properties), len(m2.Properties))
	}
}
