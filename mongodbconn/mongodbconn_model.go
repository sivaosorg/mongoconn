package mongodbconn

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
)

type MongoDB struct {
	conn       *mongo.Client     `json:"-"`
	db         *mongo.Database   `json:"-"`
	collection *mongo.Collection `json:"-"`
	bucket     *gridfs.Bucket    `json:"-"`
}
