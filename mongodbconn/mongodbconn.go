package mongodbconn

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/mongodb"
	"github.com/sivaosorg/govm/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	instance *MongoDB
	_logger  = logger.NewLogger()
)

func NewMongodb() *MongoDB {
	m := &MongoDB{}
	return m
}

func (m *MongoDB) SetConn(conn *mongo.Client) *MongoDB {
	m.conn = conn
	return m
}

func (m *MongoDB) SetDatabase(db *mongo.Database) *MongoDB {
	m.db = db
	return m
}

func (m *MongoDB) SetCollection(collection *mongo.Collection) *MongoDB {
	m.collection = collection
	return m
}

func (m *MongoDB) SetRawCollection(collection string) *MongoDB {
	m.SetCollection(m.db.Collection(collection))
	return m
}

func (m *MongoDB) SetBucket(value *gridfs.Bucket) *MongoDB {
	m.bucket = value
	return m
}

func (m *MongoDB) SetRawBucket(db string) *MongoDB {
	bucket, err := gridfs.NewBucket(
		m.conn.Database(m.db.Name()),
	)
	if err == nil {
		m.SetBucket(bucket)
	}
	return m
}

func (m *MongoDB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := m.conn.Disconnect(ctx)
	if err != nil {
		return err
	}
	return nil
}

func NewClient(config mongodb.MongodbConfig) (*MongoDB, dbx.Dbx) {
	s := dbx.NewDbx().SetDatabase(config.Database)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("Mongodb unavailable").
			SetError(fmt.Errorf(s.Message))
		return &MongoDB{}, *s
	}
	if instance != nil {
		s.SetConnected(true).SetNewInstance(false)
		return instance, *s
	}
	if config.TimeoutSecondsConn <= 0 {
		config.SetTimeoutSecondsConn(10)
	}
	if config.AllowConnSync {
		var mongoOnce sync.Once
		mongoOnce.Do(func() {
			_instance, state := getConn(config, s)
			instance = _instance
			s = &state
		})
	} else {
		_instance, state := getConn(config, s)
		instance = _instance
		s = &state
	}
	if config.DebugMode {
		_logger.Info(fmt.Sprintf("Mongodb client connection:: %s", config.Json()))
		if s.IsConnected {
			_logger.Info(fmt.Sprintf("Connected successfully to mongodb:: %s (database: %s)", getUrlConn(config), config.Database))
		}
	}
	return instance, *s
}

func getUrlConn(config mongodb.MongodbConfig) string {
	if utils.IsNotEmpty(config.UrlConn) {
		return config.UrlConn
	}
	if utils.IsNotEmpty(config.Username) && utils.IsNotEmpty(config.Password) {
		form := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.Database)
		return form
	}
	form := fmt.Sprintf("mongodb://%s:%d/%s", config.Host, config.Port, config.Database)
	return form
}

func getConn(config mongodb.MongodbConfig, s *dbx.Dbx) (*MongoDB, dbx.Dbx) {
	_options := options.Client().ApplyURI(getUrlConn(config))
	client, err := mongo.Connect(context.Background(), _options)
	if err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		return &MongoDB{}, *s
	}
	if err := client.Ping(context.Background(), readpref.Primary()); err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		return &MongoDB{}, *s
	}
	pid := os.Getpid()
	s.SetConnected(true).SetNewInstance(true).SetMessage("Connection established").SetPid(pid)
	db := client.Database(config.Database)
	instance := NewMongodb().SetConn(client).SetDatabase(db)
	bucket, err := gridfs.NewBucket(
		client.Database(config.Database),
	)
	if err == nil {
		instance.SetBucket(bucket)
	}
	return instance, *s
}
