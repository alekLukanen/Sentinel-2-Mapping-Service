package main

import (
	"fmt"
	"flag"
	"os"
	"log"
    "context"

    endpoints "core_service/endpoints"
    worker "core_service/worker"
    database "core_service/database"
)

const PORT = 7000

func main() {
	log.Println("- Initializing core geo service")

    // create the temp file directory if it does not exist
    appTempPath := "./appTemp"
    err := os.MkdirAll(appTempPath, os.ModePerm)
    if err != nil {
        log.Println("failed to create temp directory")
        log.Fatal(err)
    }

	apiCmd := flag.NewFlagSet("api", flag.ExitOnError)
    apiName := apiCmd.String("name", "", "name")

    workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
    workerName := workerCmd.String("name", "", "name")

    if len(os.Args) < 2 {
        log.Fatal("expected a subcommand")
    }

    // configure all models
    ctx := context.Background()

    if err := database.Configure(ctx); err != nil {
        log.Fatal("failed to configure database")
    } 

    log.Println("--- App Fully Configured ---")

    switch os.Args[1] {
    case "api":
        apiCmd.Parse(os.Args[2:])
        log.Println("- starting an api by name", *apiName)

		server := endpoints.SetupEndpoints(ctx, PORT)
        endpoints.StartServer(server)

    case "worker":
        workerCmd.Parse(os.Args[2:])
        fmt.Println("- starting a worker by name", *workerName)

        worker.WorkerClient(ctx)
        
    default:
        log.Fatal("expected a subcommand like api or worker")
    }
}



