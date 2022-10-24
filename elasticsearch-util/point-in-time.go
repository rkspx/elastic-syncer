package esutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

var ErrPITNotFound = errors.New("point-in-time ID not found")

type PointInTime struct {
	ID string `json:"id"`

	b *bytes.Reader
}

func (n PointInTime) Read(p []byte) (int, error) {
	if n.b == nil {
		b, err := json.Marshal(n)
		if err != nil {
			return 0, err
		}

		n.b = bytes.NewReader(b)
	}

	return n.b.Read(p)
}

func ParseOpenPIT(res *esapi.Response) (string, error) {
	defer res.Body.Close()
	if res.IsError() {
		return "", ParseCommonError(res.Body)
	}

	var pit PointInTime
	if err := json.NewDecoder(res.Body).Decode(&pit); err != nil {
		return "", err
	}

	return pit.ID, nil
}

type ClosePITResponse struct {
	Succeeded bool `json:"succeeded"`
	NumFreed  int  `json:"num_freed"`
}

func ParseClosePIT(res *esapi.Response) error {
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == http.StatusNotFound {
			return ErrPITNotFound
		}

		return ParseCommonError(res.Body)
	}

	var resp ClosePITResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return err
	}

	if !resp.Succeeded {
		return ErrPITNotFound
	}

	return nil
}
