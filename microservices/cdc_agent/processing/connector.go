package processing

import (
	"encoding/json"
	"microservices/libraries/custom_errors"
	"microservices/libraries/data"
	"microservices/libraries/models"
	"os"
	"time"

	"github.com/antrad1978/cdc_shared"
)


func ProcessStatic() {
	var syncs []cdc_shared.Sync
	_, err := os.Getwd()
	custom_errors.LogAndDie(err)
	staticFilePath := os.Getenv(models.StaticFilePath)
	dat, err2 := os.ReadFile(staticFilePath)
	custom_errors.LogAndDie(err2)

	json.Unmarshal(dat, &syncs)
	for {
		for _, sync := range syncs {
			data.SyncData(sync, sync.Mode)
		}
		time.Sleep(1 * 3600 * time.Second)
	}
}