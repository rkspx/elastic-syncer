package syncer

import (
	"bytes"
	"encoding/json"
)

type Document struct {
	DocumentMetadata
	Source json.RawMessage `json:"_source"`

	r *bytes.Reader
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
