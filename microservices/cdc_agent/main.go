package main

import (
	"fmt"
	"microservices/cdc_agent/processing"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("./.env")
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	//if custom_errors.IsStaticRunMode() {
		processing.ProcessStatic()
	//} else {
	// processing.StartWorkerNode()
	//}
}
