package main

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/pkg/errors"
)

func producer(config *kafka.ProducerConfig, docChan <-chan *model.Document) error {
	prod, err := kafka.NewProducer(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating Event-Producer")
		log.Println(err)
		return err
	}

	go func() {
		for prodErr := range prod.Errors() {
			if prodErr != nil && prodErr.Err != nil {
				parsedErr := errors.Wrap(prodErr.Err, "Error in Producer")
				log.Println(parsedErr)
				log.Println(err)
			}
		}
	}()

	go func() {
		for doc := range docChan {
			if doc == nil {
				continue
			}
			marshalInput, err := json.Marshal(doc)
			if err != nil {
				err = errors.Wrap(err, "Error Marshalling Input")
				log.Println(err)
				continue
			}
			msg := kafka.CreateMessage(doc.Topic, marshalInput)
			prod.Input() <- msg
		}
	}()

	return nil
}
