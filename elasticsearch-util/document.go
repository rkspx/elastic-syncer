package esutil

import (
	"bytes"
	"encoding/json"
)

type Document struct {
	DocumentMetadata
	Source json.RawMessage `json:"_source"`
	SortMetadata

	r *bytes.Reader
}

func (d Document) Parse(v any) error {
	return json.Unmarshal(d.Source, v)
}

type DocumentMetadata struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

func (d Document) Read(p []byte) (int, error) {
	if d.r == nil {
		b, err := json.Marshal(d.Source)
		if err != nil {
			return 0, err
		}

		d.r = bytes.NewReader(b)
	}

	return d.r.Read(p)
}

type SortMetadata struct {
	Sort []any `json:"sort"`
}

func less(v1, v2 any) bool {
	if n, ok := v1.(int64); ok {
		if m, ok := v2.(int64); ok {
			return n < m
		}
	}

	if str1, ok := v1.(string); ok {
		if str2, ok := v2.(string); ok {
			return str1 < str2
		}
	}

	return false
}

type SortBySortMetadata []Document

func (s SortBySortMetadata) Len() int { return len(s) }

func (s SortBySortMetadata) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SortBySortMetadata) Less(i, j int) bool {
	mi := s[i].SortMetadata
	mj := s[j].SortMetadata

	return less(mi, mj)
}
