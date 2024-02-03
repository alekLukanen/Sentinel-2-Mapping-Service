package endpoints

import (
	"fmt"
	"io"
	"net/http"
	"encoding/json"
	"regexp"
	"log"

	db "core_service/database"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)


type UserPostRequestBody struct {
	Name 		string		`json:"name"`
	Password	string		`json:"password"`
}
func (el *UserPostRequestBody) toUser() db.User {
	return db.User{Name:el.Name, Password: el.Password, Enabled: true, MaxAllowedBoundaries: 10, MaxAllowedBoundaryCreations: 100, BoundariesCreated: 0}
}

func postUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers","Content-Type,access-control-allow-origin, access-control-allow-headers")
	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	defer r.Body.Close()
	bodyData, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var newUserData UserPostRequestBody
	err = json.Unmarshal(bodyData, &newUserData)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	user := newUserData.toUser()
	if len(regexp.MustCompile(`[^a-zA-Z0-9]+`).FindAllString(user.Name, -1)) != 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if (len(user.Password) < 8) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "password less than 8 characters")
		return
	}

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// limit the number of users
	if userCount, err := db.UserCollection(dbClient).CountDocuments(ctx, bson.D{}); err == nil && userCount >= 1_000 {
		log.Println("hit max users!")
		w.WriteHeader(http.StatusConflict)
		return
	} else if (err != nil) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "error checking user count")
		return
	}

	userFilter := bson.D{
		{"name", user.Name},
	}
	userOps := options.Find()
	existingUsers, err := db.FindUsers(ctx, dbClient, userFilter, userOps)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(*existingUsers) != 0 {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, "user name already exists")
		return
	}

	err = db.SaveUser(ctx, dbClient, &user)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// now return a token so the user can start using the service
	validToken, err := db.GenerateJWT(user.Name)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, validToken)
}


func authUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers","Content-Type,access-control-allow-origin, access-control-allow-headers")
	if r.Method == "OPTIONS" {
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	bodyData, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var userData UserPostRequestBody
	err = json.Unmarshal(bodyData, &userData)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	
	userFilter := bson.D{{"name", userData.Name}}
	user, err := db.FindUser(ctx, dbClient, userFilter)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if user == nil {
		log.Println("user nil")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}


	passwordIsValid := db.CheckPasswordHash(userData.Password, user.Password)
	if !passwordIsValid {
		log.Println("password invalid")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	validToken, err := db.GenerateJWT(userData.Name)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, validToken)
}


func refreshUserToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

}


func IsAuthorized(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers","Content-Type,access-control-allow-origin, access-control-allow-headers, token")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		if r.Method == "OPTIONS" {
			return
		}

		if r.Header["Token"] == nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		ctx := r.Context()
		dbClient, err := db.DefaultDatabaseClient(ctx)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if user, err := db.JWTTokenUser(ctx, dbClient, r.Header["Token"][0]); err == nil && user.Enabled {
			handler.ServeHTTP(w, r)
			return
		}
		
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
}
