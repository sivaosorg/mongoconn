package mongodbconn

import (
	"context"
	"fmt"
	"sync"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/mongodb"
	"github.com/sivaosorg/govm/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	instance *mongo.Client
	_logger  = logger.NewLogger()
)

func NewClient(config mongodb.MongodbConfig) (*mongo.Client, dbx.Dbx) {
	s := dbx.NewDbx().SetDatabase(config.Database)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("Mongodb unavailable").
			SetError(fmt.Errorf(s.Message))
		return &mongo.Client{}, *s
	}
	if instance != nil {
		s.SetConnected(true).SetNewInstance(false)
		return instance, *s
	}
	if config.TimeoutSecondsConn <= 0 {
		config.SetTimeoutSecondsConn(10)
	}
	if config.AllowConnSync {
		// Used to execute client creation procedure only once.
		var mongoOnce sync.Once
		mongoOnce.Do(func() {
			_instance, state := getConn(config, instance, s)
			instance = _instance
			s = &state
		})
	} else {
		_instance, state := getConn(config, instance, s)
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

func getConn(config mongodb.MongodbConfig, instance *mongo.Client, s *dbx.Dbx) (*mongo.Client, dbx.Dbx) {
	_options := options.Client().ApplyURI(getUrlConn(config))
	client, err := mongo.Connect(context.Background(), _options)
	if err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance = nil
		return instance, *s
	}
	if err := client.Ping(context.Background(), readpref.Primary()); err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance = nil
		return instance, *s
	}
	s.SetConnected(true).SetNewInstance(true).SetMessage("Connection established")
	instance = client
	return instance, *s
}
