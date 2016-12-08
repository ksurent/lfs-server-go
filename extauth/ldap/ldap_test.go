package ldap

import (
	"testing"
	"time"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/extauth/ldap/testldap"
)

var (
	testUser   = "admin"
	testPass   = "admin"
	testServer = "127.0.0.1:1389"
)

var cfg = &config.LdapConfig{
	Enabled:         true,
	Server:          "ldap://" + testServer,
	Base:            "o=company",
	UserObjectClass: "posixaccount",
	UserCn:          "uid",
}

func TestAuthenticateLdap(t *testing.T) {
	teardown := setupLdapServer()
	defer teardown()

	ok, err := AuthenticateLdap(cfg, testUser, testPass)
	if !ok {
		if err != nil {
			t.Errorf("expected authentication to succeed, got: %s", err)
		} else {
			t.Error("expected authentication to succeed for test user")
		}
	}
}

func setupLdapServer() func() {
	s := testldap.NewServer()

	teardown := func() {
		s = nil
	}

	// TODO figure out a way to get a random free port
	go s.ListenAndServe(testServer)

	// this is stupid
	time.Sleep(2 * time.Second)

	return teardown
}
