package endpoints

import (
	"fmt"
	"net/http"
	"encoding/json"
	"os"
	"log"

	db "core_service/database"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)


type RastersResponse struct {
	Rasters []db.Raster  `json:"rasters"`
} 


func getRasters(w http.ResponseWriter, r *http.Request) {
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

	filters := bson.D{{"boundary_id", boundaryObjectId}, {"user_id", user.ID}}
	queryOpts := options.Find()
	rasters, err := db.FindRasters(ctx, dbClient, filters, queryOpts)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	responseObj := RastersResponse{Rasters:*rasters}
	responseData, err := json.Marshal(responseObj)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(responseData))
}


func getRasterImage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	vars := mux.Vars(r)
	rasterId, hasRasterId := vars["rasterId"]
	if !hasRasterId {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rasterObjectId, err := primitive.ObjectIDFromHex(rasterId)
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

	filters := bson.D{{"_id", rasterObjectId}, {"user_id", user.ID}}
	raster, err := db.FindRaster(ctx, dbClient, filters)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	dir, err := os.MkdirTemp(db.TEMP_DIR, "raster_image_data")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(dir) // clean up

	rasterImageData, err := raster.RetrieveRasterImage(ctx, dir)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, rasterImageData)
}