package mongoconn

import (
	"github.com/sivaosorg/govm/dbx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
)

type MongoDB struct {
	conn       *mongo.Client
	db         *mongo.Database
	collection *mongo.Collection
	bucket     *gridfs.Bucket
	State      dbx.Dbx `json:"state"`
}
