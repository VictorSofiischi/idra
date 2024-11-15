package main

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Sync struct {
	ID                   string               `json:"id"`
	Mode                 string               `json:"mode"`
	SyncName             string               `json:"sync_name"`
	SourceConnector      SourceConnector      `json:"source_connector"`
	DestinationConnector DestinationConnector `json:"destination_connector"`
}

type DestinationConnector struct {
	ID                   string            `json:"id"`
	Query                *string           `json:"query,omitempty"`
	Table                string            `json:"table"`
	SaveMode             any               `json:"save_mode"`
	PollingTime          *int              `json:"polling_time,omitempty"`
	ConnectorName        string            `json:"connector_name"`
	TimestampField       string            `json:"timestamp_field"`
	ConnectionString     string            `json:"connection_string"`
	Attributes           map[string]string `json:"attributes,omitempty"`
	ConnectorSourceType  any               `json:"connector_source_type"`
	MaxRecordBatchSize   *int              `json:"max_record_batch_size,omitempty"`
	TimestampFieldFormat string            `json:"timestamp_field_format"`
}

type SourceConnector struct {
	ID                   string            `json:"id"`
	Query                string            `json:"query"`
	Table                string            `json:"table"`
	SaveMode             any               `json:"save_mode,omitempty"`
	PollingTime          int               `json:"polling_time"`
	ConnectorName        string            `json:"connector_name"`
	TimestampField       string            `json:"timestamp_field"`
	ConnectionString     string            `json:"connection_string"`
	Attributes           map[string]string `json:"attributes,omitempty"`
	ConnectorSourceType  any               `json:"connector_source_type"`
	MaxRecordBatchSize   int               `json:"max_record_batch_size"`
	TimestampFieldFormat string            `json:"timestamp_field_format"`
}

// func TestInsertRowsKafka(t *testing.T){
// 	manager := data2.PostgresGormManager{}
// 	connector := cdc_shared.Connector{}
// 	connector.Table = "table"
// 	connector.IdField = "id"
// 	connector.ConnectionString = os.Getenv("KAFKA_URL")
// 	rows, _ := manager.GetRowsById(connector, 0)

// 	kafka := data2.KafkaConnector{}
// 	connector2 := cdc_shared.Connector{}
// 	connector2.Table = "table"
// 	connector2.IdField = "id"
// 	kafka.InsertRows(connector2, rows)
// }

func TestTransmitingJsonThroughKafka(t *testing.T) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					t.Errorf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					t.Logf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	send_topic := "kafkaTest"
	fileData, err := os.ReadFile("sample_kafka.json")
	if err != nil {
		t.Errorf("Failed to read JSON file: %s", err)
	}

	var msgData []Sync
	if err := json.Unmarshal(fileData, &msgData); err != nil {
		t.Errorf("Failed to unmarshal JSON data: %s", err)
	}

	messagePayload, err := json.Marshal(msgData)
	if err != nil {
		t.Errorf("Failed to marshal data to JSON: %s", err)
	}

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &send_topic, Partition: kafka.PartitionAny},
		Value:          messagePayload,
	}, nil)

	p.Flush(5000)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Errorf("%v", err)
	}

	err = c.SubscribeTopics([]string{send_topic}, nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	run := true


	for run {
	msg, err := c.ReadMessage(time.Second)
	if err == nil {
		fileData, err := os.ReadFile("sample_kafka.json")
		if err != nil {
			t.Errorf("Failed to read JSON file: %s", err)
		}

		var kafkaMessage, fileMessage []Sync;
		if err := json.Unmarshal(msg.Value, &kafkaMessage); err != nil {
			t.Errorf("Failed to unmarshal Kafka message JSON data: %s", err)
		}

		if err := json.Unmarshal(fileData, &fileMessage); err != nil {
			t.Errorf("Failed to unmarshal File message JSON data: %s", err)
		}

		if(reflect.DeepEqual(kafkaMessage, fileMessage)){
			t.Log("Test passed, messages are the same")
		}else {
			t.Error("Failed - differite messages")
		}
		run = false;
		continue;
	} else if !err.(kafka.Error).IsTimeout() {
		t.Errorf("Consumer error: %v (%v)\n", err, msg)
	}
	}


	c.Close()
}