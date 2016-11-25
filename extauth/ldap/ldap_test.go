package ldap

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/ksurent/lfs-server-go/config"

	l "github.com/nmcclain/ldap"
)

var (
	testUser = "admin"
	testPass = "admin"
)

func TestNewLdapConnection(t *testing.T) {
	setupMetaAuth()
	defer tearDownMetaAuth()
	lh, err := ldapHost()
	if err != nil {
		t.Errorf("Unable to process LDAP host %s", err.Error())
	}
	if lh.Host != "localhost:1389" {
		t.Errorf("Wrong ldap host. expected localhost but got %s", lh.Host)
	}

	_, err = NewLdapConnection()
	if err != nil {
		t.Errorf("Errored trying to connect to ldap, %s", err.Error())
	}

	lbind, _ := LdapBind(testUser, testPass)
	if !lbind {
		t.Errorf("Failed to bind as %s", testUser)
	}

}
func TestLdapBind(t *testing.T) {
	setupMetaAuth()
	defer tearDownMetaAuth()

	lbind, _ := LdapBind(testUser, testPass)
	if !lbind {
		t.Errorf("Failed to bind as %s", testUser)
	}

	lbind, _ = LdapBind(testUser, "badpass")
	if lbind {
		t.Errorf("Bound as %s but it should have failed", testUser)
	}

}

func TestLdapSearch(t *testing.T) {
	setupMetaAuth()
	defer tearDownMetaAuth()
	fltr := fmt.Sprintf("(&(objectClass=%s)(%s=%s))", config.Config.Ldap.UserObjectClass, config.Config.Ldap.UserCn, testUser)
	base := fmt.Sprintf("%s=%s,%s", config.Config.Ldap.UserCn, testUser, config.Config.Ldap.Base)
	search := &l.SearchRequest{
		BaseDN: base,
		Filter: fltr,
	}
	lsearch, err := LdapSearch(search)
	if err != nil {
		t.Errorf("Failed looking for user %s error: %s", testUser, err.Error())
	}
	found := false
	for _, e := range lsearch.Entries {
		if strings.Contains(e.DN, testUser) {
			found = true
		}
	}
	if !found {
		t.Errorf("Failed to find user %s error: %s", testUser, err.Error())
	}
}

func tearDownMetaAuth() error {
	// Set back to defaults
	config.Config.Ldap = &config.LdapConfig{Enabled: false, Server: "ldap://localhost:1389", Base: "dc=testers,c=test,o=company",
		UserObjectClass: "objectclass=person", UserCn: "uid"}
	exec.Command("pkill test_ldap_server").Run()
	return nil
}
func setupMetaAuth() error {
	config.Config.Ldap = &config.LdapConfig{Enabled: true, Server: "ldap://localhost:1389", Base: "o=company",
		UserObjectClass: "posixaccount", UserCn: "uid", BindPass: "admin"}
	rme := exec.Command("test_ldap_server/test_ldap_server")
	wd, _ := os.Getwd()
	rme.Dir = wd
	rme.Start() // run and forget about it
	return nil
}
