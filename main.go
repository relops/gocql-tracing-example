package main

import (
	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
)

type TraceWriter struct {
	session *gocql.Session
}

func (t *TraceWriter) Write(p []byte) (n int, err error) {
	log.Info(string(p[:len(p)-1]))
	return len(p), nil
}

func init() {
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")
	if err != nil {
		panic(err)
	}

	logger.SetAdditionalStackDepth(1)
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

	writer := &TraceWriter{session: traceSession}
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
