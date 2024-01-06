package mongoconn

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongodbService interface {
	Create(ctx context.Context, model interface{}) error
	Find(ctx context.Context, filter interface{}, model interface{}) error
	FindAll(ctx context.Context, models interface{}) error
	Update(ctx context.Context, filter interface{}, update interface{}) error
	Delete(ctx context.Context, filter interface{}) error
	Filter(ctx context.Context, filter interface{}, models interface{}) error
	Count(ctx context.Context, filter interface{}) (int64, error)
	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, options *options.FindOneAndUpdateOptions, result interface{}) error
	FindOneAndDelete(ctx context.Context, filter interface{}, options *options.FindOneAndDeleteOptions, result interface{}) error
	FindOne(ctx context.Context, filter interface{}, options *options.FindOneOptions, result interface{}) error
	FindWithOptions(ctx context.Context, filter interface{}, options *options.FindOptions, models interface{}) error
	ModelName() string
	CreateMany(ctx context.Context, models []interface{}) error
	BulkWrite(ctx context.Context, writes []mongo.WriteModel) (*mongo.BulkWriteResult, error)
	Distinct(ctx context.Context, fieldName string, filter interface{}) ([]interface{}, error)
	Aggregate(ctx context.Context, pipeline interface{}, results interface{}) error
	FindOneAndUpdateWithOptions(ctx context.Context, filter interface{}, update interface{}, options *options.FindOneAndUpdateOptions, result interface{}) error
	FindOneAndDeleteWithOptions(ctx context.Context, filter interface{}, options *options.FindOneAndDeleteOptions, result interface{}) error
	FindWithOptionsReturnCursor(ctx context.Context, filter interface{}, projection interface{}, opts *options.FindOptions) (*mongo.Cursor, error)
	WithTransaction(ctx context.Context, transactionFunc func(ctx context.Context) error) error
	CreateIndexWithOptions(ctx context.Context, keys interface{}, options options.IndexOptions) error
	ListIndexes(ctx context.Context) ([]bson.M, error)
	BackupDatabase(ctx context.Context, filename string) error
	RestoreDatabase(ctx context.Context, inputPath string) error
	UploadFile(ctx context.Context, filename string, content io.Reader) (primitive.ObjectID, error)
	DownloadFile(ctx context.Context, objectID primitive.ObjectID) (*bytes.Buffer, error)
	ListAllDocuments(ctx context.Context) ([]bson.M, error)
}

type mongodbServiceImpl struct {
	mongodbConn *MongoDB
}

func NewMongodbService(mongodbConn *MongoDB) MongodbService {
	s := &mongodbServiceImpl{
		mongodbConn: mongodbConn,
	}
	return s
}

func (m *mongodbServiceImpl) Create(ctx context.Context, model interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	_, err := m.mongodbConn.collection.InsertOne(ctx, model)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) Find(ctx context.Context, filter interface{}, model interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOne(ctx, filter).Decode(model)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindAll(ctx context.Context, models interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, models)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) Update(ctx context.Context, filter interface{}, update interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	updateResult, err := m.mongodbConn.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if updateResult.MatchedCount == 0 {
		return fmt.Errorf("no matching document")
	}
	return nil
}

func (m *mongodbServiceImpl) Delete(ctx context.Context, filter interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	deleteResult, err := m.mongodbConn.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	if deleteResult.DeletedCount == 0 {
		return fmt.Errorf("no matching document")
	}
	return nil
}

func (m *mongodbServiceImpl) Filter(ctx context.Context, filter interface{}, models interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, models)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) Count(ctx context.Context, filter interface{}) (int64, error) {
	if m.mongodbConn.collection == nil {
		return -1, fmt.Errorf("Missing collection")
	}
	count, err := m.mongodbConn.collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *mongodbServiceImpl) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, options *options.FindOneAndUpdateOptions, result interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOneAndUpdate(ctx, filter, update, options).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindOneAndDelete(ctx context.Context, filter interface{}, options *options.FindOneAndDeleteOptions, result interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOneAndDelete(ctx, filter, options).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindOne(ctx context.Context, filter interface{}, options *options.FindOneOptions, result interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOne(ctx, filter, options).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindWithOptions(ctx context.Context, filter interface{}, options *options.FindOptions, models interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Find(ctx, filter, options)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, models)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) ModelName() string {
	return m.mongodbConn.collection.Name()
}

func (m *mongodbServiceImpl) CreateMany(ctx context.Context, models []interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	if len(models) == 0 {
		return nil
	}
	docs := make([]interface{}, 0, len(models))
	for _, model := range models {
		docs = append(docs, model)
	}
	_, err := m.mongodbConn.collection.InsertMany(ctx, docs)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) BulkWrite(ctx context.Context, writes []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
	if m.mongodbConn.collection == nil {
		return nil, fmt.Errorf("Missing collection")
	}
	res, err := m.mongodbConn.collection.BulkWrite(ctx, writes)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *mongodbServiceImpl) Distinct(ctx context.Context, fieldName string, filter interface{}) ([]interface{}, error) {
	if m.mongodbConn.collection == nil {
		return nil, fmt.Errorf("Missing collection")
	}
	res, err := m.mongodbConn.collection.Distinct(ctx, fieldName, filter)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *mongodbServiceImpl) Aggregate(ctx context.Context, pipeline interface{}, results interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, results)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindOneAndUpdateWithOptions(ctx context.Context, filter interface{}, update interface{}, options *options.FindOneAndUpdateOptions, result interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOneAndUpdate(ctx, filter, update, options).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindOneAndDeleteWithOptions(ctx context.Context, filter interface{}, options *options.FindOneAndDeleteOptions, result interface{}) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	err := m.mongodbConn.collection.FindOneAndDelete(ctx, filter, options).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) FindWithOptionsReturnCursor(ctx context.Context, filter interface{}, projection interface{}, opts *options.FindOptions) (*mongo.Cursor, error) {
	if m.mongodbConn.collection == nil {
		return nil, fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}

func (m *mongodbServiceImpl) WithTransaction(ctx context.Context, transactionFunc func(ctx context.Context) error) error {
	session, err := m.mongodbConn.conn.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	callback := func(sessionCtx mongo.SessionContext) (interface{}, error) {
		err := transactionFunc(sessionCtx)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) CreateIndexWithOptions(ctx context.Context, keys interface{}, options options.IndexOptions) error {
	if m.mongodbConn.collection == nil {
		return fmt.Errorf("Missing collection")
	}
	_, err := m.mongodbConn.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    keys,
		Options: &options,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *mongodbServiceImpl) ListIndexes(ctx context.Context) ([]bson.M, error) {
	if m.mongodbConn.collection == nil {
		return nil, fmt.Errorf("Missing collection")
	}
	cursor, err := m.mongodbConn.collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var indexes []bson.M
	err = cursor.All(ctx, &indexes)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

// checking if needed
func (m *mongodbServiceImpl) BackupDatabase(ctx context.Context, filename string) error {
	cmd := bson.D{
		{Key: "mongodump", Value: "/usr/bin/mongodump"},
		{Key: "db", Value: m.mongodbConn.db.Name()},
		{Key: "out", Value: filename},
	}
	result := bson.M{}
	err := m.mongodbConn.conn.Database("admin").RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return err
	}
	if result["ok"] == 1 {
		return nil
	}
	return fmt.Errorf("backup failed: %v", result["errmsg"])
}

func (m *mongodbServiceImpl) RestoreDatabase(ctx context.Context, filename string) error {
	cmd := bson.M{
		"mongorestore": 1,
		"db":           m.mongodbConn.db.Name(),
		"dir":          filename,
	}
	result := bson.M{}
	err := m.mongodbConn.conn.Database("admin").RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return err
	}
	if result["ok"] == 1 {
		return nil
	}
	return fmt.Errorf("restore failed: %v", result["errmsg"])
}

func (m *mongodbServiceImpl) UploadFile(ctx context.Context, filename string, content io.Reader) (primitive.ObjectID, error) {
	if m.mongodbConn.bucket == nil {
		return primitive.NilObjectID, fmt.Errorf("Missing bucket")
	}
	stream, err := m.mongodbConn.bucket.OpenUploadStream(filename)
	if err != nil {
		return primitive.NilObjectID, err
	}
	defer stream.Close()
	objectID := stream.FileID.(primitive.ObjectID)
	_, err = io.Copy(stream, content)
	if err != nil {
		return primitive.NilObjectID, err
	}
	return objectID, nil
}

func (m *mongodbServiceImpl) DownloadFile(ctx context.Context, objectID primitive.ObjectID) (*bytes.Buffer, error) {
	if m.mongodbConn.bucket == nil {
		return nil, fmt.Errorf("Missing bucket")
	}
	stream, err := m.mongodbConn.bucket.OpenDownloadStream(objectID)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	buffer := new(bytes.Buffer)
	_, err = buffer.ReadFrom(stream)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (m *mongodbServiceImpl) ListAllDocuments(ctx context.Context) ([]bson.M, error) {
	if m.mongodbConn.collection == nil {
		return nil, fmt.Errorf("Missing collection")
	}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{}}},
		bson.D{{Key: "$project", Value: bson.M{"_id": 0}}},
	}
	cursor, err := m.mongodbConn.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var result []bson.M
	if err := cursor.All(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}
