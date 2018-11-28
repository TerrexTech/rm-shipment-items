package test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	cmodel "github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/rm-shipment-items/model"
	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rm-shipment-items/connutil"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestQuery tests Query-handling.
func TestQuery(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",
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
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "QueryHandler Suite")
}

var _ = Describe("ItemsReadModel", func() {
	var (
		coll *mongo.Collection

		reqTopic  string
		respTopic string

		prodInput  chan<- *sarama.ProducerMessage
		consConfig *kafka.ConsumerConfig
	)

	BeforeSuite(func() {
		var err error
		coll, err = connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())

		reqTopic = os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
		respTopic = "test.response"

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)
		prod, err := kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		prodInput = prod.Input()

		consConfig = &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			Topics:       []string{respTopic},
			GroupName:    "test.group.1",
		}
	})

	It("should timeout query if it exceeds TTL", func(done Done) {
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockCmd := cmodel.Command{
			Action:        "LoginItem",
			CorrelationID: cid,
			Data:          []byte("{}"),
			ResponseTopic: respTopic,
			Source:        "test-source",
			SourceTopic:   reqTopic,
			Timestamp:     time.Now().Add(-15 * time.Second).UTC().Unix(),
			TTLSec:        15,
			UUID:          uuid,
		}
		marshalCmd, err := json.Marshal(mockCmd)
		Expect(err).ToNot(HaveOccurred())

		msg := kafka.CreateMessage(reqTopic, marshalCmd)
		prodInput <- msg

		consumer, err := kafka.NewConsumer(consConfig)
		Expect(err).ToNot(HaveOccurred())

		success := true
		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			doc := &cmodel.Document{}
			err := json.Unmarshal(msg.Value, doc)
			Expect(err).ToNot(HaveOccurred())

			if doc.CorrelationID == mockCmd.UUID {
				success = false
				return true
			}
			return false
		}

		timeout := time.Duration(10) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		handler := &msgHandler{msgCallback}
		err = consumer.Consume(ctx, handler)
		Expect(err).ToNot(HaveOccurred())

		err = consumer.Close()
		Expect(err).To(HaveOccurred())
		Expect(success).To(BeTrue())
		close(done)
	}, 15)

	Describe("Latest", func() {
		It("should get latest items as specified in Count", func(done Done) {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}

			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			cmdData, err := json.Marshal(map[string]interface{}{
				"count": 1,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockCmd := cmodel.Command{
				Action:        "Latest",
				CorrelationID: cid,
				Data:          cmdData,
				ResponseTopic: respTopic,
				Source:        "test-source",
				SourceTopic:   reqTopic,
				Timestamp:     time.Now().UTC().Unix(),
				TTLSec:        15,
				UUID:          uuid,
			}
			marshalCmd, err := json.Marshal(mockCmd)
			Expect(err).ToNot(HaveOccurred())

			msg := kafka.CreateMessage(reqTopic, marshalCmd)
			prodInput <- msg

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				doc := &cmodel.Document{}
				err := json.Unmarshal(msg.Value, doc)
				Expect(err).ToNot(HaveOccurred())

				if doc.CorrelationID == mockCmd.UUID {
					Expect(doc.Error).To(BeEmpty())
					Expect(doc.ErrorCode).To(BeZero())

					items := []model.Item{}
					err = json.Unmarshal(doc.Data, &items)
					Expect(err).ToNot(HaveOccurred())
					Expect(items).To(HaveLen(1))

					for _, item := range items {
						if item.ItemID == mockItem.ItemID {
							Expect(mockItem).To(Equal(mockItem))
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 10)
	})

	Describe("Timestamp", func() {
		It("should get items by timestamp as specified in Count", func(done Done) {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}

			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			cmdData, err := json.Marshal(map[string]interface{}{
				"start": mockItem.Timestamp - 3,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockCmd := cmodel.Command{
				Action:        "Timestamp",
				CorrelationID: cid,
				Data:          cmdData,
				ResponseTopic: respTopic,
				Source:        "test-source",
				SourceTopic:   reqTopic,
				Timestamp:     time.Now().UTC().Unix(),
				TTLSec:        15,
				UUID:          uuid,
			}
			marshalCmd, err := json.Marshal(mockCmd)
			Expect(err).ToNot(HaveOccurred())

			msg := kafka.CreateMessage(reqTopic, marshalCmd)
			prodInput <- msg

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				doc := &cmodel.Document{}
				err := json.Unmarshal(msg.Value, doc)
				Expect(err).ToNot(HaveOccurred())

				if doc.CorrelationID == mockCmd.UUID {
					Expect(doc.Error).To(BeEmpty())
					Expect(doc.ErrorCode).To(BeZero())

					items := []model.Item{}
					err = json.Unmarshal(doc.Data, &items)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(items)).To(BeNumerically("<", 51))

					for _, item := range items {
						if item.ItemID == mockItem.ItemID {
							Expect(mockItem).To(Equal(mockItem))
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 10)
	})

	Describe("Items", func() {
		It("should get items by filter-params as specified in Count", func(done Done) {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			lotID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          lotID.String(),
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}

			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			cmdData, err := json.Marshal(map[string]interface{}{
				"filter": map[string]interface{}{
					"lot": mockItem.Lot,
				},
				"count": 4,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockCmd := cmodel.Command{
				Action:        "Items",
				CorrelationID: cid,
				Data:          cmdData,
				ResponseTopic: respTopic,
				Source:        "test-source",
				SourceTopic:   reqTopic,
				Timestamp:     time.Now().UTC().Unix(),
				TTLSec:        15,
				UUID:          uuid,
			}
			marshalCmd, err := json.Marshal(mockCmd)
			Expect(err).ToNot(HaveOccurred())

			msg := kafka.CreateMessage(reqTopic, marshalCmd)
			prodInput <- msg

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				doc := &cmodel.Document{}
				err := json.Unmarshal(msg.Value, doc)
				Expect(err).ToNot(HaveOccurred())

				if doc.CorrelationID == mockCmd.UUID {
					Expect(doc.Error).To(BeEmpty())
					Expect(doc.ErrorCode).To(BeZero())

					items := []model.Item{}
					err = json.Unmarshal(doc.Data, &items)
					Expect(err).ToNot(HaveOccurred())

					for _, item := range items {
						Expect(item.Lot).To(Equal(mockItem.Lot))
					}
					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 10)
	})

	Describe("OrQuery", func() {
		It("should get items matching any of the params as specified in Count", func(done Done) {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem1 := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}
			_, err = coll.InsertOne(mockItem1)
			Expect(err).ToNot(HaveOccurred())

			itemID, err = uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err = uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem2 := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}
			_, err = coll.InsertOne(mockItem2)
			Expect(err).ToNot(HaveOccurred())

			cmdData, err := json.Marshal(map[string]interface{}{
				"filter": []map[string]interface{}{
					map[string]interface{}{
						"itemID": mockItem1.ItemID,
					},
					map[string]interface{}{
						"rsCustomerID": mockItem2.RSCustomerID,
					},
				},
				"count": 4,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockCmd := cmodel.Command{
				Action:        "OrQuery",
				CorrelationID: cid,
				Data:          cmdData,
				ResponseTopic: respTopic,
				Source:        "test-source",
				SourceTopic:   reqTopic,
				Timestamp:     time.Now().UTC().Unix(),
				TTLSec:        15,
				UUID:          uuid,
			}
			marshalCmd, err := json.Marshal(mockCmd)
			Expect(err).ToNot(HaveOccurred())

			msg := kafka.CreateMessage(reqTopic, marshalCmd)
			prodInput <- msg

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				doc := &cmodel.Document{}
				err := json.Unmarshal(msg.Value, doc)
				Expect(err).ToNot(HaveOccurred())

				if doc.CorrelationID == mockCmd.UUID {
					Expect(doc.Error).To(BeEmpty())
					Expect(doc.ErrorCode).To(BeZero())

					items := []model.Item{}
					err = json.Unmarshal(doc.Data, &items)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(items)).To(Equal(2))

					success1 := false
					success2 := false
					for _, item := range items {
						if item == mockItem1 {
							success1 = true
						}
						if item == mockItem1 {
							success2 = true
						}
						if success1 && success2 {
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 10)
	})
})
