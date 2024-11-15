package main

import (
	"fmt"
	"microservices/cdc_agent/processing"

	"microservices/libraries/custom_errors"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("./.env")
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	if custom_errors.IsStaticRunMode() {
		processing.ProcessStatic()
	} else {
		processing.StartWorkerNode()
	}
}
