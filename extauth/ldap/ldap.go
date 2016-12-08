package ldap

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/ksurent/lfs-server-go/config"

	l "github.com/nmcclain/ldap"
)

var (
	errLdapUserNotFound    = errors.New("Unable to find user in LDAP")
	errNoLdapSearchResults = errors.New("No results from LDAP")
	errLdapSearchFailed    = errors.New("Failed searching LDAP")
)

func AuthenticateLdap(cfg *config.LdapConfig, user, password string) (bool, error) {
	conn, err := connect(cfg.Server)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	dn, err := findUserDn(conn, cfg.Base, cfg.UserObjectClass, cfg.UserCn, user)
	if err != nil {
		return false, err
	}

	reqE := conn.Bind(dn, password)
	return reqE == nil, nil
}

func connect(rawurl string) (*l.Conn, error) {
	url, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if !strings.Contains(url.Host, ":") {
		url.Host += ":389"
	}

	if url.Scheme == "ldaps" {
		return l.DialTLS("tcp", url.Host, &tls.Config{InsecureSkipVerify: true})
	}

	return l.Dial("tcp", url.Host)
}

func findUserDn(conn *l.Conn, base, userClass, userCn, user string) (string, error) {
	req := &l.SearchRequest{
		BaseDN:     base,
		Filter:     fmt.Sprintf("(&(objectclass=%s)(%s=%s))", userClass, userCn, user),
		Scope:      1,
		Attributes: []string{"dn"},
	}

	res, err := conn.Search(req)
	if err != nil {
		return "", err
	}

	if len(res.Entries) > 0 {
		return res.Entries[0].DN, nil
	}

	return "", errLdapUserNotFound
}
