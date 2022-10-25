package esutil

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type IndexSetting struct {
	Index   string
	Setting SettingInner
}

func (i IndexSetting) Parse() ([]byte, error) {
	return json.Marshal(i.Setting)
}

func (i IndexSetting) Replicas() int {
	return stringToInt(i.Setting.Settings.Index.NumberOfReplicas)
}

func (i IndexSetting) Shards() int {
	return stringToInt(i.Setting.Settings.Index.NumberOfShards)
}

type SettingInner struct {
	Aliases  Aliases  `json:"aliases"`
	Mappings Mappings `json:"mappings"`
	Settings Settings `json:"settings"`
}

type Aliases = map[string]any

type Mappings struct {
	Properties map[string]MappingProperty `json:"properties,omitempty"`
}

type MappingProperty struct {
	Type       string                              `json:"type,omitempty"`
	Fields     map[string]MappingPropertyFieldType `json:"fields,omitempty"`
	Properties map[string]MappingProperty          `json:"properties,omitempty"`
}

type MappingPropertyFieldType struct {
	Type        string `json:"type"`
	IgnoreAbove int    `json:"ignore_above"`
}

type Settings struct {
	Index IndexSettingInner `json:"index"`
}

type IndexSettingInner struct {
	NumberOfShards   string `json:"number_of_shards"`
	NumberOfReplicas string `json:"number_of_replicas"`
}

func ParseIndicesGetResponse(res *esapi.Response) ([]IndexSetting, error) {
	defer res.Body.Close()
	if res.IsError() {
		return nil, ParseIndicesErrorResponse(res.Body)
	}

	var results map[string]SettingInner
	if err := json.NewDecoder(res.Body).Decode(&results); err != nil {
		return nil, err
	}

	settings := make([]IndexSetting, 0, len(results))
	for index, setting := range results {
		settings = append(settings, IndexSetting{
			Index:   index,
			Setting: setting,
		})
	}

	return settings, nil
}

type IndicesGetErrorResponse struct {
	Status int             `json:"status"`
	Err    IndicesGetError `json:"error"`
}

type IndicesGetError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	Index  string `json:"index"`
}

func (e IndicesGetErrorResponse) Error() string {
	return fmt.Sprintf("%d: %s - %s (%s)", e.Status, e.Err.Type, e.Err.Reason, e.Err.Index)
}

func ParseIndicesErrorResponse(r io.Reader) error {
	var e IndicesGetErrorResponse
	if err := json.NewDecoder(r).Decode(&e); err != nil {
		return err
	}

	return e
}
