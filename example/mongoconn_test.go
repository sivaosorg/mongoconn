package example

import (
	"testing"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/mongodb"
	"github.com/sivaosorg/mongoconn"
)

func createConn() (*mongoconn.MongoDB, dbx.Dbx) {
	return mongoconn.NewClient(*mongodb.GetMongodbConfigSample())
}

func TestConn(t *testing.T) {
	_, s := createConn()
	logger.Infof("Mongo connection status: %v", s)
}
