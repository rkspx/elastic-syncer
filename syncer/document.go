package syncer

import "encoding/json"

type Document struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Source json.RawMessage `json:"_source"`
}

type IndexSetting struct {
	Index   string
	Setting SettingInner
}

type SettingInner struct {
	Aliases  Aliases  `json:"aliases"`
	Mappings Mappings `json:"mappings"`
	Settings Settings `json:"settings"`
}

type Aliases = map[string]any

type Mappings struct {
	Properties map[string]struct {
		Type   string `json:"type"`
		Fields struct {
			Keyword struct {
				Type        string `json:"keyword"`
				IgnoreAbove int    `json:"ignore_above"`
			} `json:"keyword,omitempty"`
		} `json:"fields"`
	} `json:"properties"`
}

type Settings struct {
	NumberOfShards   string `json:"number_of_shards"`
	NumberOfReplicas string `json:"number_of_replicas"`
}
