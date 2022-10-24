package esutil

import (
	"encoding/json"
	"fmt"
	"io"
)

type CommonErrorResponse struct {
	Err    CommonError `json:"error"`
	Status int         `json:"status"`
}

func (e CommonErrorResponse) Error() string {
	return fmt.Sprintf("%d: %s", e.Status, e.Err.String())
}

type CommonError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	Index  string `json:"index,omitempty"`
}

func (e CommonError) String() string {
	if e.Index != "" {
		return fmt.Sprintf("%s - %s (%s)", e.Type, e.Index, e.Reason)
	}

	return fmt.Sprintf("%s- %s", e.Type, e.Reason)
}

func ParseCommonError(r io.Reader) error {
	var commonError CommonErrorResponse
	if err := json.NewDecoder(r).Decode(&commonError); err != nil {
		return err
	}

	return commonError
}
