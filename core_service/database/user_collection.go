package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

var JWT_SECRET_KEY string

func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		JWT_SECRET_KEY = "default-secret-key-for-testing"
	} else {
		JWT_SECRET_KEY = GetEnvironmentVariableOrPanic("JWT_SECRET_KEY")
	}
}

type User struct {
	ID                          primitive.ObjectID `bson:"_id" json:"id"`
	Name                        string             `bson:"name" json:"name"`
	Password                    string             `bson:"password" json:"password"`
	MaxAllowedBoundaries        int32              `bson:"max_allowed_boundaries" json:"maxAllowedBoundaries"`
	MaxAllowedBoundaryCreations int32              `bson:"max_allowed_boundary_creations" json:"maxAllowedBoundaryCreations"`
	BoundariesCreated           int32              `bson:"boundaries_created" json:"boundariesCreated"`
	Enabled                     bool               `bson:"enabled" json:"enabled"`
}

func UserCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(DatabaseName()).Collection("user")
}

func FindUser(ctx context.Context, client *mongo.Client, filter bson.D) (*User, error) {
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	coll := UserCollection(client)
	opts := options.FindOne()

	var user User
	err := coll.FindOne(mongoCtx, filter, opts).Decode(&user)

	if err != nil {
		return nil, err
	}
	return &user, nil
}

func FindUsers(ctx context.Context, client *mongo.Client, filter bson.D, opts *options.FindOptions) (*[]User, error) {
	coll := UserCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 60*time.Second)
	defer mongoCancel()

	users := make([]User, 0, 100)
	cursor, err := coll.Find(mongoCtx, filter, opts)
	if err = cursor.All(mongoCtx, &users); err != nil {
		return nil, err
	}

	return &users, err
}

func SaveUser(ctx context.Context, client *mongo.Client, user *User) error {
	coll := UserCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	user.ID = primitive.NewObjectID()
	hashedPassword, err := GeneratehashPassword(user.Password)
	if err != nil {
		return err
	}
	user.Password = hashedPassword
	_, err = coll.InsertOne(mongoCtx, user)
	if err != nil {
		return err
	}
	return nil
}

func IncrementUserBoundaryCreateCount(ctx context.Context, client *mongo.Client, user *User) error {
	coll := UserCollection(client)
	mongoCtx, mongoCancel := context.WithTimeout(ctx, 15*time.Second)
	defer mongoCancel()

	userFilter := bson.D{
		{"_id", user.ID},
	}
	userUpdate := bson.D{
		{"$set", bson.D{
			{"boundaries_created", user.BoundariesCreated + 1},
		}},
	}

	_, err := coll.UpdateOne(mongoCtx, userFilter, userUpdate)
	if err != nil {
		return err
	}
	return nil
}

func UnmarshalJsonUser(data []byte) (*User, error) {
	var userData User
	err := json.Unmarshal(data, &userData)
	return &userData, err
}

func MarshalJsonUser(user *User) ([]byte, error) {
	jsonBytes, err := json.Marshal(user)
	return jsonBytes, err
}

func GeneratehashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}

func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func GenerateJWT(name string) (string, error) {
	var mySigningKey = []byte(JWT_SECRET_KEY)
	token := jwt.New(jwt.SigningMethodHS256)
	claims := token.Claims.(jwt.MapClaims)

	claims["authorized"] = true
	claims["name"] = name
	claims["exp"] = time.Now().Add(time.Minute * 30).Unix()

	tokenString, err := token.SignedString(mySigningKey)

	if err != nil {
		fmt.Errorf("Something Went Wrong: %s", err.Error())
		return "", err
	}
	return tokenString, nil
}

func JWTTokenUser(ctx context.Context, client *mongo.Client, tokenValue string) (*User, error) {
	var mySigningKey = []byte(JWT_SECRET_KEY)

	token, err := jwt.Parse(tokenValue, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("There was an error in parsing")
		}
		return mySigningKey, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		name := claims["name"]
		userFilters := bson.D{{"name", name}}
		user, err := FindUser(ctx, client, userFilters)
		if err != nil {
			return nil, err
		}
		return user, nil
	}

	return nil, errors.New("invalid token")
}
