package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/pkg/errors"
)

type queryConsHandler struct {
	handler func(*model.Command)
}

func (*queryConsHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka queryConsHandler")
	return nil
}

func (*queryConsHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka queryConsHandler")
	return nil
}

func (m *queryConsHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for Queries...")

	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			session.MarkMessage(msg, "")

			query := &model.Command{}
			err := json.Unmarshal(msg.Value, query)
			if err != nil {
				err = errors.Wrap(err, "Error unmarshalling to Command")
				log.Println(err)
				return
			}
			log.Printf("Received Query with ID: %s", query.UUID)

			if query.ResponseTopic == "" {
				log.Println("Query contains empty ResponseTopic")
				return
			}
			if query.Action == "" {
				log.Println("Comman contains empty Action")
				return
			}

			ttlSec := time.Duration(query.TTLSec) * time.Second
			expTime := time.Unix(query.Timestamp, 0).Add(ttlSec).UTC()
			curTime := time.Now().UTC()
			if expTime.Before(curTime) {
				log.Printf("Query expired, ignoring")
				return
			}

			m.handler(query)
		}(session, msg)
	}
	return errors.New("context-closed")
}
