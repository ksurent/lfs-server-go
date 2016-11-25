package ldap

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/ksurent/lfs-server-go/config"

	l "github.com/nmcclain/ldap"
)

var (
	errLdapUserNotFound    = errors.New("Unable to find user in LDAP")
	errNoLdapSearchResults = errors.New("No results from LDAP")
	errLdapSearchFailed    = errors.New("Failed searching LDAP")

	ErrUseLdap = errors.New("Not implemented when using LDAP")
)

func ldapHost() (*url.URL, error) {
	return url.Parse(config.Config.Ldap.Server)
}

func NewLdapConnection() (*l.Conn, error) {
	var err error
	lh, err := ldapHost()
	if err != nil {
		return nil, err
	}
	hoster := strings.Split(lh.Host, ":")
	port := func() uint16 {
		if len(hoster) < 2 {
			return uint16(389)
		} else {
			var e error
			port, e := strconv.Atoi(hoster[1])
			if e != nil {
				panic(e)
			}
			return uint16(port)
		}
	}
	var ldapCon *l.Conn
	if strings.Contains(lh.String(), "ldaps") {
		ldapCon, err = l.DialTLS("tcp", fmt.Sprintf("%s:%d", hoster[0], port()), &tls.Config{InsecureSkipVerify: true})
	} else {
		ldapCon, err = l.Dial("tcp", fmt.Sprintf("%s:%d", hoster[0], port()))
	}
	if err != nil {
		return nil, err
	}
	return ldapCon, nil
}

func LdapSearch(search *l.SearchRequest) (*l.SearchResult, error) {
	ldapCon, err := NewLdapConnection()
	if err != nil {
		return nil, err
	}
	s, err := ldapCon.Search(search)
	defer ldapCon.Close()
	if err != nil {
		return nil, err
	}
	if (len(config.Config.Ldap.BindDn) + len(config.Config.Ldap.BindPass)) > 0 {
		err = ldapCon.Bind(config.Config.Ldap.BindDn, config.Config.Ldap.BindPass)
		if err != nil {
			return nil, err
		}
	}
	if len(s.Entries) == 0 {
		return nil, errNoLdapSearchResults
	}
	return s, err
}

// boolean bind request
func LdapBind(user, pass string) (bool, error) {
	ldapCon, err := NewLdapConnection()
	if err != nil {
		return false, err
	}
	defer ldapCon.Close()

	reqE := ldapCon.Bind(user, pass)

	return reqE == nil, nil
}

// authenticate uses the authorization string to determine whether
// or not to proceed. This server assumes an HTTP Basic auth format.
func AuthenticateLdap(user, password string) (bool, error) {
	dn, err := findUserDn(user)
	if err != nil {
		return false, err
	}
	return LdapBind(dn, password)
}

func findUserDn(user string) (string, error) {
	//	fmt.Printf("Looking for user '%s'\n", user)
	fltr := fmt.Sprintf("(&(objectclass=%s)(%s=%s))", config.Config.Ldap.UserObjectClass, config.Config.Ldap.UserCn, user)
	//	m := fmt.Sprintf("LDAP Search \"ldapsearch -x -H '%s' -b '%s' '%s'\"\n", config.Config.Ldap.Server, config.Config.Ldap.Base, fltr)
	search := &l.SearchRequest{
		BaseDN:     config.Config.Ldap.Base,
		Filter:     fltr,
		Scope:      1,
		Attributes: []string{"dn"},
	}
	r, err := LdapSearch(search)
	if err != nil {
		return "", err
	}
	if len(r.Entries) > 0 {
		return r.Entries[0].DN, nil
	}
	return "", errLdapUserNotFound
}
