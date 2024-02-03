package endpoints

import (
	"fmt"
	"io"
	"net/http"
	"log"

	db "core_service/database"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func getPatchDeleteBoundary(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		getBoundary(w, r)
	} else if r.Method == "PATCH" {
		w.WriteHeader(http.StatusNotImplemented)
		return
	} else if r.Method == "DELETE" {
		deleteBoundary(w, r)
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}


func getBoundary(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	vars := mux.Vars(r)
	boundaryId, hasBoundaryId := vars["boundaryId"]
	if !hasBoundaryId {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	boundaryObjectId, err := primitive.ObjectIDFromHex(boundaryId)
	if err != nil{
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	user, err := db.JWTTokenUser(ctx, dbClient, r.Header["Token"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	filters := bson.D{{"_id", boundaryObjectId}, {"user_id", user.ID}}
	boundary, err := db.FindBoundary(ctx, dbClient, filters)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	boundaryData, err := db.MarshalJsonBoundary(boundary)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(boundaryData))
}


func postBoundary(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()
	bodyData, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	
	if len(bodyData) > 5000 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "boundary request too large")
		return
	}

	boundaryObj, err := db.UnmarshalJsonBoundary(bodyData)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	user, err := db.JWTTokenUser(ctx, dbClient, r.Header["Token"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if user.MaxAllowedBoundaryCreations <= user.BoundariesCreated {
		w.WriteHeader(http.StatusConflict)
		return
	}

	boundaryObj.UserId = user.ID
	boundaryObj.MgrsCodes = db.ComputeMgrsCodesFromGeometry(&boundaryObj.Geometry)
	if len(boundaryObj.MgrsCodes) != 1 {
		// only allow boundaries that fall into a single mgrs code
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if boundaryArea, err := db.ComputeBoundaryArea(&boundaryObj.Geometry); err == nil {
		boundaryObj.Acres = boundaryArea / 4046.8564224
		if boundaryObj.Acres > 2500 || boundaryObj.Acres <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "boundary area too large")
			return
		}
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// limit the number of boundaries a user can have
	totalBoundaries, err := db.BoundaryCollection(dbClient).CountDocuments(ctx, bson.D{{"user_id", user.ID}})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else if (totalBoundaries >= int64(user.MaxAllowedBoundaries)) {
		w.WriteHeader(http.StatusConflict)
		return
	}

	err = db.SaveBoundary(ctx, dbClient, boundaryObj)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// update the users boundary creation count
	if err := db.IncrementUserBoundaryCreateCount(ctx, dbClient, user); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// publish an event to build the map
	event := db.Event{
		EventType: "BuildBoundaryMapTask",
		MaxAttemps: 1,
		Priority: 5,
		Data: map[string]string{
			"mgrsCode": boundaryObj.MgrsCodes[0],
			"boundaryId": boundaryObj.ID.Hex(),
		},
	}
	err = db.SaveEvent(ctx, dbClient, &event)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data, err := db.MarshalJsonBoundary(boundaryObj)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(data))
}

func getBoundaries(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	user, err := db.JWTTokenUser(ctx, dbClient, r.Header["Token"][0])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	filters := bson.D{{"user_id", user.ID}}
	opts := options.Find()
	boundaries, err := db.FindBoundaries(ctx, dbClient, filters, opts)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	boundaryData, err := db.MarshalJsonBoundaries(boundaries)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(boundaryData))
}


func deleteBoundary(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	vars := mux.Vars(r)
	boundaryID, hasBoundaryID := vars["boundaryId"]
	if !hasBoundaryID {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	boundaryObjectID, err := primitive.ObjectIDFromHex(boundaryID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dbClient, err := db.DefaultDatabaseClient(ctx)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	
	user, err := db.JWTTokenUser(ctx, dbClient, r.Header.Get("Token"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	filters := bson.D{{"_id", boundaryObjectID}, {"user_id", user.ID}}
	err = db.DeleteBoundary(ctx, dbClient, filters)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
