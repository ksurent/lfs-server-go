package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/content"
	"github.com/ksurent/lfs-server-go/logger"
	"github.com/ksurent/lfs-server-go/meta"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/gorilla/context"
	"github.com/gorilla/mux"
)

// Representation is object metadata as seen by clients of the lfs server.
type Representation struct {
	Oid   string           `json:"oid"`
	Size  int64            `json:"size"`
	Links map[string]*link `json:"_links"`
}

// link provides a structure used to build a hypermedia representation of an HTTP link.
type link struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
}

// App links a Router, ContentStore, and MetaStore to provide the LFS server.
type App struct {
	router       *mux.Router
	contentStore content.GenericContentStore
	metaStore    meta.GenericMetaStore
}

// NewApp creates a new App using the ContentStore and MetaStore provided
func NewApp(c content.GenericContentStore, m meta.GenericMetaStore) *App {
	app := &App{contentStore: c, metaStore: m}

	r := mux.NewRouter()

	r.HandleFunc("/debug/vars", app.DebugHandler).Methods("GET")

	add(r, "/{namespace}/{repo}/objects/batch", app.BatchHandler, metaResponse).Methods("POST").MatcherFunc(MetaMatcher)
	add(r, "/{namespace}/{repo}/objects", app.PostHandler, metaResponse).Methods("POST").MatcherFunc(MetaMatcher)
	add(r, "/search/{oid}", app.GetSearchHandler, metaResponse).Methods("GET")
	add(r, "/{namespace}/{repo}/verify", app.VerifyHandler, metaResponse).Methods("POST").MatcherFunc(ContentMatcher)

	route := "/{namespace}/{repo}/objects/{oid}"

	add(r, route, app.GetMetaHandler, metaResponse).Methods("GET", "HEAD").MatcherFunc(MetaMatcher)
	add(r, route, app.GetContentHandler, downloadResponse).Methods("GET", "HEAD").MatcherFunc(ContentMatcher)
	add(r, route, app.PutHandler, uploadResponse).Methods("PUT").MatcherFunc(ContentMatcher)

	app.addMgmt(r)

	app.router = r

	return app
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err == nil {
		context.Set(r, "RequestID", fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]))
	}

	a.router.ServeHTTP(w, r)

	go totalRequests.Add(1)
}

func (a *App) Serve() error {
	srv := &http.Server{
		Addr:    config.Config.Listen,
		Handler: a,
	}

	if config.Config.UseTLS() {
		logger.Log("Using TLS")

		tlsCfg := &tls.Config{
			NextProtos:   []string{"http/1.1"},
			Certificates: make([]tls.Certificate, 1),
		}

		if pair, err := tls.LoadX509KeyPair(config.Config.Cert, config.Config.Key); err == nil {
			tlsCfg.Certificates[0] = pair
		} else {
			logger.Fatal(err)
		}

		srv.TLSConfig = tlsCfg
	}

	return gracehttp.Serve(srv)
}

// GetContentHandler gets the content from the content store
func (a *App) GetContentHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	m, err := a.metaStore.Get(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	reader, err := a.contentStore.Get(m)
	if err != nil {
		logger.Log(err)

		return notFound(w, r)
	}
	defer reader.Close()

	io.Copy(w, reader)

	return http.StatusOK
}

// GetSearchHandler (search handler used by pre-push hooks)
func (a *App) GetSearchHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	_, err := a.metaStore.Get(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	return http.StatusOK
}

// GetMetaHandler retrieves metadata about the object
func (a *App) GetMetaHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	m, err := a.metaStore.Get(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	w.Header().Set("Content-Type", metaMediaType)

	if r.Method == "GET" {
		enc := json.NewEncoder(w)
		enc.Encode(a.Represent(rv, m, true, false, false))
	}

	return http.StatusOK
}

// PostHandler instructs the client how to upload data (legacy API)
func (a *App) PostHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	m, err := a.metaStore.Put(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	w.Header().Set("Content-Type", metaMediaType)

	sentStatus := 202
	if m.Existing && a.contentStore.Exists(m) {
		sentStatus = 200
	}
	w.WriteHeader(sentStatus)

	enc := json.NewEncoder(w)
	enc.Encode(a.Represent(rv, m, m.Existing, true, true))

	if !m.Existing {
		go metaPending.Add(1)
	}

	return sentStatus
}

// BatchHandler provides the batch api
func (a *App) BatchHandler(w http.ResponseWriter, r *http.Request) int {
	bv := unpackbatch(r)

	var responseObjects []*Representation

	for _, object := range bv.Objects {
		// Put() checks if the object already exists in the meta store and
		// returns it if it does
		m, err := a.metaStore.Put(object)
		if err != nil {
			logger.Log(err)

			if meta.IsAuthError(err) {
				return requireAuth(w, r)
			}

			continue
		}

		responseObjects = append(
			responseObjects,
			a.Represent(object, m, m.Existing, !m.Existing, true),
		)

		if !m.Existing {
			go metaPending.Add(1)
		}
	}

	w.Header().Set("Content-Type", metaMediaType)

	type ro struct {
		Objects []*Representation `json:"objects"`
	}

	respobj := &ro{responseObjects}

	enc := json.NewEncoder(w)
	enc.Encode(respobj)

	return http.StatusOK
}

// PutHandler receives data from the client and puts it into the content store
func (a *App) PutHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	m, err := a.metaStore.GetPending(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	if err := a.contentStore.Put(m, r.Body); err != nil {
		logger.Log(err)

		return http.StatusInternalServerError
	}

	_, err = a.metaStore.Commit(rv)
	if err != nil {
		logger.Log(err)

		return http.StatusInternalServerError
	}

	go metaPending.Add(-1)

	return http.StatusOK
}

func (a *App) VerifyHandler(w http.ResponseWriter, r *http.Request) int {
	rv := unpack(r)
	m, err := a.metaStore.Get(rv)
	if err != nil {
		logger.Log(err)

		if meta.IsAuthError(err) {
			return requireAuth(w, r)
		}
		return notFound(w, r)
	}

	w.Header().Set("Content-Type", metaMediaType)

	status := http.StatusNotFound
	if a.contentStore.Verify(m) == nil {
		status = http.StatusOK
	}

	return status
}

func (a *App) DebugHandler(w http.ResponseWriter, r *http.Request) {
	// from expvar.go, since the expvarHandler isn't exported :(
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// Represent takes a meta.RequestVars and Meta and turns it into a Representation suitable
// for json encoding
func (a *App) Represent(rv *meta.RequestVars, m *meta.Object, download, upload, verify bool) *Representation {
	rep := &Representation{
		Oid:   m.Oid,
		Size:  m.Size,
		Links: make(map[string]*link),
	}

	header := make(map[string]string)
	header["Accept"] = contentMediaType
	if rv.Authorization != "" {
		header["Authorization"] = rv.Authorization
	}

	if download {
		rep.Links["download"] = &link{Href: rv.ObjectLink(), Header: header}
	}

	if upload {
		rep.Links["upload"] = &link{Href: rv.ObjectLink(), Header: header}
	}

	if verify {
		rep.Links["verify"] = &link{Href: rv.VerifyLink(), Header: header}
	}

	return rep
}

// ContentMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the contentMediaType
func ContentMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == contentMediaType
}

// MetaMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the metaMediaType
func MetaMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == metaMediaType
}

func unpack(r *http.Request) *meta.RequestVars {
	vars := mux.Vars(r)
	rv := &meta.RequestVars{
		Namespace:     vars["namespace"],
		Repo:          vars["repo"],
		Oid:           vars["oid"],
		Authorization: r.Header.Get("Authorization"),
	}

	if r.Method == "POST" { // Maybe also check if +json
		var p meta.RequestVars
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&p)
		if err != nil {
			return rv
		}

		rv.Oid = p.Oid
		rv.Size = p.Size
	}

	return rv
}

// TODO cheap hack, unify with unpack
func unpackbatch(r *http.Request) *meta.BatchVars {
	vars := mux.Vars(r)

	var bv meta.BatchVars

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&bv)
	if err != nil {
		return &bv
	}

	for i := 0; i < len(bv.Objects); i++ {
		bv.Objects[i].Namespace = vars["namespace"]
		bv.Objects[i].Repo = vars["repo"]
		bv.Objects[i].Authorization = r.Header.Get("Authorization")
	}

	return &bv
}

func logRequest(r *http.Request, status int) {
	logger.Log(fmt.Sprintf(
		"rid=%s status=%d method=%s url=%s",
		context.Get(r, "RequestID"),
		status,
		r.Method,
		r.URL,
	))
}

func writeStatus(w http.ResponseWriter, r *http.Request, status int) {
	message := http.StatusText(status)

	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	if strings.HasSuffix(mt, "+json") {
		message = `{"message":"` + message + `"}`
	}

	w.WriteHeader(status)
	fmt.Fprint(w, message)
}

func notFound(w http.ResponseWriter, r *http.Request) int {
	writeStatus(w, r, http.StatusNotFound)
	return http.StatusNotFound
}

func requireAuth(w http.ResponseWriter, r *http.Request) int {
	w.Header().Set("Lfs-Authenticate", "Basic realm=lfs-server-go")
	writeStatus(w, r, http.StatusUnauthorized)
	return http.StatusUnauthorized
}

func add(r *mux.Router, path string, f func(http.ResponseWriter, *http.Request) int, exp *expvar.Map) *mux.Route {
	wrapped := func(w http.ResponseWriter, r *http.Request) {
		status := f(w, r)
		logRequest(r, status)
		go exp.Add(strconv.Itoa(status), 1)
	}

	return r.HandleFunc(path, wrapped)
}
