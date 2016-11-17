package meta

import (
	"fmt"

	"github.com/ksurent/lfs-server-go/config"
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

func (v *RequestVars) ObjectLink() string {
	path := fmt.Sprintf("/%s/%s/objects/%s", v.Namespace, v.Repo, v.Oid)

	if config.Config.IsHTTPS() {
		return fmt.Sprintf("%s://%s%s", config.Config.Scheme, config.Config.Host, path)
	}

	return fmt.Sprintf("http://%s%s", config.Config.Host, path)
}

func (v *RequestVars) VerifyLink() string {
	path := fmt.Sprintf("/%s/%s/verify", v.Namespace, v.Repo)

	if config.Config.IsHTTPS() {
		return fmt.Sprintf("%s://%s%s", config.Config.Scheme, config.Config.Host, path)
	}

	return fmt.Sprintf("http://%s%s", config.Config.Host, path)
}
