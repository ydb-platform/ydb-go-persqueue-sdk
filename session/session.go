package session

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-persqueue-sdk/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

const (
	defaultPort    = 2135
	magicCookie    = 123456789
	maxMessageSize = 1024 * 1024 * 130
	clientTimeout  = 15 * time.Second
	rootDatabase   = "/Root"
	metaDatabase   = "x-ydb-database"
	metaAuthTicket = "x-ydb-auth-ticket"
)

var (
	ErrDiscoveryNotReady = errors.New("endpoint discovery operations returned not ready status; " +
		"this should never happen")
)

type Options struct {
	Endpoint      string
	Port          int
	Credentials   credentials.Credentials
	TLSConfig     *tls.Config
	Logger        log.Logger
	proxy         string
	Database      string
	ClientTimeout time.Duration

	DiscoverCluster      bool
	Topic                string
	SourceID             []byte
	PartitionGroup       uint32
	PreferredClusterName string
}

func (s *Options) endpoint() string {
	port := defaultPort
	if s.Port != 0 {
		port = s.Port
	}
	return formatEndpoint(s.Endpoint, port)
}

func (s *Options) database() string {
	if s.Database != "" {
		return s.Database
	}
	return rootDatabase
}

func (s *Options) clientTimeout() time.Duration {
	if s.ClientTimeout == 0 {
		return clientTimeout
	}
	return s.ClientTimeout
}

func (s Options) WithProxy(proxy string) Options {
	s.proxy = proxy
	return s
}

func printIssues(issues []*Ydb_Issue.IssueMessage) string {
	res := ""
	for _, issue := range issues {
		childIssues := printIssues(issue.GetIssues())
		if issue.GetMessage() != "" {
			res = fmt.Sprintf("\n%v", issue.GetMessage())
		}
		if childIssues != "" {
			res = fmt.Sprintf("\n%v", childIssues)
		}
	}
	return res
}
