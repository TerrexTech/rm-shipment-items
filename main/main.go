package main

import (
	"context"
	"log"
	"os"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/rm-shipment-items/connutil"
	"github.com/TerrexTech/rm-shipment-items/query"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func validateEnv() error {
	log.Println("Reading environment file")
	err := godotenv.Load("./.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_GROUP_REQUEST",
		"KAFKA_CONSUMER_TOPIC_REQUEST",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",

		"MONGO_DATABASE",
		"MONGO_COLLECTION",
		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		return err
	}
	return nil
}

func main() {
	err := validateEnv()
	if err != nil {
		log.Fatalln(err)
	}

	collection, err := connutil.LoadMongoConfig()
	if err != nil {
		err = errors.Wrap(err, "Error getting mongo-collection")
		log.Fatalln(err)
	}

	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

	queryGroup := os.Getenv("KAFKA_CONSUMER_GROUP_REQUEST")
	queryTopic := os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
	queryCons, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    queryGroup,
		KafkaBrokers: kafkaBrokers,
		Topics:       []string{queryTopic},
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating query-consumer")
		log.Fatalln(err)
	}

	resultChan := make(chan *model.Document, 256)
	err = producer(
		&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		},
		(<-chan *model.Document)(resultChan),
	)
	if err != nil {
		err = errors.Wrap(err, "Error creating producer")
		log.Fatalln(err)
	}

	svcName := os.Getenv("SERVICE_NAME")
	queryHandler, err := query.NewHandler(&query.HandlerConfig{
		Coll:        collection,
		ServiceName: svcName,
		ResultProd:  (chan<- *model.Document)(resultChan),
	})
	if err != nil {
		err = errors.Wrap(err, "Error initializing Query-Handler")
		log.Fatalln(err)
	}

	consHandler := &queryConsHandler{
		handler: queryHandler.Handle,
	}
	err = queryCons.Consume(context.Background(), consHandler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to cosume Queries")
		log.Fatalln(err)
	}
}
