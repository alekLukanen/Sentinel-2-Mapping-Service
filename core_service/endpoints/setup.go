package endpoints

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"

	db "core_service/database"
)

var UI_BUILD_PATH string

func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		UI_BUILD_PATH = ""
	} else {
		UI_BUILD_PATH = db.GetEnvironmentVariable("UI_BUILD_PATH")
	}
}

func SetupEndpoints(ctx context.Context, port int) *http.Server {

	r := mux.NewRouter()

	// api routes
	r.HandleFunc("/api/alive", alive)
	r.HandleFunc("/api/boundary/{boundaryId}", IsAuthorized(getPatchDeleteBoundary)).Methods("GET", "PATCH", "DELETE", "OPTIONS")
	r.HandleFunc("/api/boundary", IsAuthorized(postBoundary)).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/boundary", IsAuthorized(getBoundaries)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/boundary/{boundaryId}/rasters", IsAuthorized(getRasters)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/raster/image/{rasterId}", IsAuthorized(getRasterImage)).Methods("GET", "OPTIONS")
	r.HandleFunc("/api/signup", postUser).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/signin", authUser).Methods("POST", "OPTIONS")
	r.HandleFunc("/api/refreshToken", IsAuthorized(refreshUserToken)).Methods("POST", "OPTIONS")

	// ui routes
	// the react app is a single page app with a router so we need
	// to serve the index.html file for all routes prefixed with /r/
	if UI_BUILD_PATH != "" {
		fs := http.FileServer(http.Dir(UI_BUILD_PATH))
		indexFile := path.Join(UI_BUILD_PATH, "index.html")
		r.HandleFunc("/r/{pageName}", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, indexFile)
		})
		r.PathPrefix("/").Handler(fs)

		log.Println("UI build path: ", UI_BUILD_PATH)
	} else {
		log.Println("UI build path not set")
	}

	address := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr:    address,
		Handler: r,
		BaseContext: func(l net.Listener) context.Context {
			ctx = context.WithValue(ctx, address, l.Addr().String())
			return ctx
		},
	}
	return server
}

func StartServer(server *http.Server) {
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		log.Fatal("server one closed")
	} else if err != nil {
		log.Fatalf("error listening for server: %s", err)
	}
}

func alive(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Core Geo Service Alive")
}

