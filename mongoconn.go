package mongoconn

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
	_logger = logger.NewLogger()
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

func (m *MongoDB) SetState(value dbx.Dbx) *MongoDB {
	m.State = value
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

func (m *MongoDB) GetConn() *mongo.Client {
	return m.conn
}

func (m *MongoDB) GetDb() *mongo.Database {
	return m.db
}

func (m *MongoDB) GetCollection() *mongo.Collection {
	return m.collection
}

func (m *MongoDB) GetBucket() *gridfs.Bucket {
	return m.bucket
}

func (m *MongoDB) Json() string {
	return utils.ToJson(m)
}

func NewClient(config mongodb.MongodbConfig) (*MongoDB, dbx.Dbx) {
	instance := NewMongodb()
	s := dbx.NewDbx().SetDatabase(config.Database)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("Mongodb unavailable").
			SetError(fmt.Errorf(s.Message))
		instance.SetState(*s)
		return instance, *s
	}
	if config.Timeout <= 0 {
		config.SetTimeout(10 * time.Second)
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
	instance.SetState(*s)
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
	instance := NewMongodb()
	_options := options.Client().ApplyURI(getUrlConn(config))
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	client, err := mongo.Connect(ctx, _options)
	if err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance.SetState(*s)
		return instance, *s
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance.SetState(*s)
		return instance, *s
	}
	s.SetConnected(true).SetNewInstance(true).SetMessage("Connected successfully").SetPid(os.Getpid())
	db := client.Database(config.Database)
	instance.SetConn(client).SetDatabase(db)
	bucket, err := gridfs.NewBucket(
		client.Database(config.Database),
	)
	if err == nil {
		instance.SetBucket(bucket)
	} else {
		s.SetError(fmt.Errorf("Error bucket: %v", err.Error()))
	}
	instance.SetState(*s)
	return instance, *s
}
