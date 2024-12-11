package main

import (
	"fmt"
	"microservices/cdc_agent/processing"

	"microservices/libraries/custom_errors"

	"net/http"
	_ "net/http/pprof"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("./.env")
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	if custom_errors.IsStaticRunMode() {
		processing.ProcessStatic()
	} else {
		processing.StartWorkerNode()
	}
}
