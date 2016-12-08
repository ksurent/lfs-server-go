package meta

import (
	"fmt"
)

// RequestVars contain variables from the HTTP request. Variables from routing, json body decoding, and
// some headers are stored.
type RequestVars struct {
	Oid           string
	Size          int64
	User          string
	Password      string
	Namespace     string
	Repo          string
	Authorization string
}

type BatchVars struct {
	Objects []*RequestVars `json:"objects"`
}

func (v *RequestVars) ObjectLink(scheme, host string) string {
	path := fmt.Sprintf("/%s/%s/objects/%s", v.Namespace, v.Repo, v.Oid)

	return fmt.Sprintf("%s://%s%s", scheme, host, path)
}

func (v *RequestVars) VerifyLink(scheme, host string) string {
	path := fmt.Sprintf("/%s/%s/verify", v.Namespace, v.Repo)

	return fmt.Sprintf("%s://%s%s", scheme, host, path)
}
