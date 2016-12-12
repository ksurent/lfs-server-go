package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/content"
	"github.com/ksurent/lfs-server-go/content/fs"
	"github.com/ksurent/lfs-server-go/meta"
	"github.com/ksurent/lfs-server-go/meta/boltdb"
)

var (
	lfsServer         *httptest.Server
	testMetaStore     meta.GenericMetaStore
	testContentStore  content.GenericContentStore
	testUser          = "admin"
	testPass          = "admin"
	contentStr        = "this is my content"
	contentSize       = int64(len(contentStr))
	contentOid        = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	nonexistingOid    = "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f"
	noAuthcontent     = "Some content goes here"
	noAuthContentSize = int64(len(noAuthcontent))
	noAuthOid         = "4609ed10888c145d228409aa5587bab9fe166093bb7c155491a96d079c9149be"
	extraRepo         = "mytestproject"
	testRepo          = "repo"
)

var cfg = &config.Configuration{
	Scheme:      "https",
	Host:        "localhost",
	Public:      false,
	Ldap:        &config.LdapConfig{Enabled: false},
	MetaDB:      "/tmp/lfs-server-go.db",
	ContentPath: "/tmp/lfs-server-go-test",
}

func TestGetAuthed(t *testing.T) {
	req, err := http.NewRequest("GET", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", contentMediaType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}

	by, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("expected response to contain content, got error: %s", err)
	}

	if string(by) != contentStr {
		t.Fatalf("expected content to be `content`, got: %s", string(by))
	}
}

func TestGetUnauthed(t *testing.T) {
	req, err := http.NewRequest("GET", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.Header.Set("Accept", contentMediaType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 401 {
		t.Fatalf("expected status 401, got %d %s", res.StatusCode, req.URL)
	}
}

func TestGetMetaAuthed(t *testing.T) {
	req, err := http.NewRequest("GET", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", metaMediaType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d %s", res.StatusCode, req.URL)
	}

	var m Representation
	dec := json.NewDecoder(res.Body)
	dec.Decode(&m)

	if m.Oid != contentOid {
		t.Fatalf("expected to see oid `%s` in meta, got: `%s`", contentOid, m.Oid)
	}

	if m.Size != contentSize {
		t.Fatalf("expected to see a size of `%d`, got: `%d`", contentSize, m.Size)
	}

	download := m.Links["download"]

	if download.Href != baseURL()+"/namespace/repo/objects/"+contentOid {
		t.Fatalf("expected download link, got %s", download.Href)
	}
}

func TestGetMetaUnauthed(t *testing.T) {
	req, err := http.NewRequest("GET", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.Header.Set("Accept", metaMediaType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 401 {
		t.Fatalf("expected status 401, got %d", res.StatusCode)
	}
}

func TestPostAuthedNewObject(t *testing.T) {
	req, err := http.NewRequest("POST", lfsServer.URL+"/namespace/repo/objects", nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", metaMediaType)

	buf := bytes.NewBufferString(fmt.Sprintf(`{"oid":"%s", "size":1234}`, nonexistingOid))
	req.Body = ioutil.NopCloser(buf)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 202 {
		t.Fatalf("expected status 202, got %d", res.StatusCode)
	}

	var m Representation
	dec := json.NewDecoder(res.Body)
	dec.Decode(&m)

	if m.Oid != nonexistingOid {
		t.Fatalf("expected to see oid `%s` in meta, got: `%s`", nonexistingOid, m.Oid)
	}

	if m.Size != 1234 {
		t.Fatalf("expected to see a size of `1234`, got: `%d`", m.Size)
	}

	if download, ok := m.Links["download"]; ok {
		fmt.Println(ok)
		t.Fatalf("expected POST to not contain a download link, got %v", download)
	}

	upload, ok := m.Links["upload"]
	if !ok {
		t.Fatal("expected upload link to be present")
	}

	if upload.Href != baseURL()+"/namespace/repo/objects/"+nonexistingOid {
		t.Fatalf("expected upload link to be %s, got %s", baseURL()+"/namespace/repo/objects/"+nonexistingOid, upload.Href)
	}
}

func TestPostAuthedExistingObject(t *testing.T) {
	req, err := http.NewRequest("POST", lfsServer.URL+"/namespace/repo/objects", nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", metaMediaType)

	buf := bytes.NewBufferString(fmt.Sprintf(`{"oid":"%s", "size":%d}`, contentOid, contentSize))
	req.Body = ioutil.NopCloser(buf)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}

	var m Representation
	dec := json.NewDecoder(res.Body)
	dec.Decode(&m)

	if m.Oid != contentOid {
		t.Fatalf("expected to see oid `%s` in meta, got: `%s`", contentOid, m.Oid)
	}

	if m.Size != contentSize {
		t.Fatalf("expected to see a size of `%d`, got: `%d`", contentSize, m.Size)
	}

	download := m.Links["download"]
	if download.Href != baseURL()+"/namespace/repo/objects/"+contentOid {
		t.Fatalf("expected download link to be %s, got %s", baseURL()+"/namespace/repo/objects/"+contentOid, download.Href)
	}

	upload, ok := m.Links["upload"]
	if !ok {
		t.Fatalf("expected upload link to be present")
	}

	if upload.Href != baseURL()+"/namespace/repo/objects/"+contentOid {
		t.Fatalf("expected upload link, got %s", upload.Href)
	}
}

func TestPostUnauthed(t *testing.T) {
	req, err := http.NewRequest("POST", lfsServer.URL+"/namespace/repo/objects", nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.Header.Set("Accept", metaMediaType)

	buf := bytes.NewBufferString(fmt.Sprintf(`{"oid":"%s", "size":%d}`, contentOid, contentSize))
	req.Body = ioutil.NopCloser(buf)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}
	if len(res.Header["Lfs-Authenticate"]) < 0 {
		t.Fatalf("expected auth to be requested but it was not")
	}
	if res.StatusCode != 401 {
		t.Fatalf("expected status 401, got %d", res.StatusCode)
	}
}

func TestPut(t *testing.T) {
	// XXX this test is currently broken

	req, err := http.NewRequest("PUT", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", contentMediaType)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Body = ioutil.NopCloser(bytes.NewBuffer([]byte(contentStr)))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}

	r, err := testContentStore.Get(&meta.Object{Oid: contentOid})
	if err != nil {
		t.Fatalf("error retreiving from content store: %s", err)
	}
	c, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("error reading content: %s", err)
	}
	if string(c) != contentStr {
		t.Fatalf("expected content, got `%s`", string(c))
	}
}

func TestMediaTypesRequired(t *testing.T) {
	m := []string{"GET", "PUT", "POST", "HEAD"}
	for _, method := range m {
		req, err := http.NewRequest(method, lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
		if err != nil {
			t.Fatalf("request error: %s", err)
		}
		req.SetBasicAuth(testUser, testPass)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("response error: %s", err)
		}

		if res.StatusCode != 404 {
			t.Fatalf("expected status 404, got %d", res.StatusCode)
		}
	}
}

func TestMediaTypesParsed(t *testing.T) {
	req, err := http.NewRequest("GET", lfsServer.URL+"/namespace/repo/objects/"+contentOid, nil)
	if err != nil {
		t.Fatalf("request error: %s", err)
	}
	req.SetBasicAuth(testUser, testPass)
	req.Header.Set("Accept", contentMediaType+"; charset=utf-8")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("response error: %s", err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}
}

func TestMain(m *testing.M) {
	os.Remove(cfg.MetaDB)
	os.RemoveAll(cfg.ContentPath)

	log.SetOutput(ioutil.Discard)

	var err error

	testMetaStore, err = boltdb.NewMetaStore(cfg.MetaDB)
	if err != nil {
		fmt.Printf("Error creating meta store: %s", err)
		os.Exit(1)
	}

	testContentStore, err = fs.NewContentStore(cfg.ContentPath)
	if err != nil {
		fmt.Printf("Error creating content store: %s", err)
		os.Exit(1)
	}

	if err = seedMetaStore(); err != nil {
		fmt.Printf("Error seeding meta store: %s", err)
		os.Exit(1)
	}

	if err = seedContentStore(); err != nil {
		fmt.Printf("Error seeding content store: %s", err)
		os.Exit(1)
	}

	app := NewApp(cfg, testContentStore, testMetaStore)

	lfsServer = httptest.NewServer(app)

	ret := m.Run()

	lfsServer.Close()
	testMetaStore.Close()

	os.Exit(ret)
}

func seedMetaStore() error {
	if err := testMetaStore.AddUser(testUser, testPass); err != nil {
		fmt.Println("Erred adding user", err.Error())
		return err
	}

	rv := &meta.RequestVars{
		Oid:  contentOid,
		Size: contentSize,
		Repo: testRepo,
	}

	if _, err := testMetaStore.Put(rv); err != nil {
		return fmt.Errorf("Put(): %s\n", err.Error())
	}
	if _, err := testMetaStore.Commit(rv); err != nil {
		return fmt.Errorf("Commit(): %s\n", err.Error())
	}

	return nil
}

func seedContentStore() error {
	m := &meta.Object{Oid: contentOid, Size: contentSize}
	buf := bytes.NewBufferString(contentStr)

	return testContentStore.Put(m, buf)
}

func baseURL() string {
	return fmt.Sprintf("%s://%s", cfg.Scheme, cfg.Host)
}
