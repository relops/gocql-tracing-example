package main

import (
	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
)

type TraceWriter struct {
	session *gocql.Session
	log     log.LoggerInterface
}

func (t *TraceWriter) Write(p []byte) (n int, err error) {
	t.log.Info(string(p[:len(p)-1]))
	return len(p), nil
}

func NewTraceWriter(s *gocql.Session, l log.LoggerInterface) *TraceWriter {
	return &TraceWriter{session: s, log: l}
}

func init() {
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")
	if err != nil {
		panic(err)
	}
	log.ReplaceLogger(logger)
}

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "system"
	session, err := cluster.CreateSession()
	defer session.Close()

	if err != nil {
		log.Critical(err)
	}

	traceSession, err := cluster.CreateSession()
	defer session.Close()

	if err != nil {
		log.Critical(err)
	}

	// Create a new logger instance that we adjust the stack depth for to get
	// more meaningful frames
	logger, err := log.LoggerFromConfigAsString(
		`<seelog>
            <outputs>
                <console formatid="fmt"/>
            </outputs>
            <formats>
                <format id="fmt" format="%Date(Jan 02 2006 03:04:05.000) [%LEVEL] %File:%Line - %Msg%n"/>
            </formats>
        </seelog>
        `)
	logger.SetAdditionalStackDepth(2)

	if err != nil {
		log.Critical(err)
	}

	writer := NewTraceWriter(traceSession, logger)
	tracer := gocql.NewTraceWriter(traceSession, writer)
	session.SetTrace(tracer)

	var count int
	iter := session.Query(`select count(*) from schema_keyspaces`).Iter()
	iter.Scan(&count)

	err = iter.Close()

	if err != nil {
		log.Critical(err)
	}

	log.Infof("This instance has %d keyspaces", count)
}
