package query

import (
	"log"

	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
)

// HandlerConfig is the config for Query-Handler.
type HandlerConfig struct {
	Coll        *mongo.Collection
	ServiceName string

	ResultProd chan<- *model.Document
}

// Handler for queries.
type Handler struct {
	*HandlerConfig
}

// NewHandler creates a new Query-Handler.
func NewHandler(config *HandlerConfig) (*Handler, error) {
	if config.Coll == nil {
		return nil, errors.New("Coll cannot be nil")
	}
	if config.ServiceName == "" {
		return nil, errors.New("ServiceName cannot be blank")
	}
	if config.ResultProd == nil {
		return nil, errors.New("ResultProd cannot be nil")
	}

	return &Handler{
		config,
	}, nil
}

// Handle handles the provided query.
func (h *Handler) Handle(query *model.Command) {
	var (
		queryErr    *model.Error
		queryResult []byte
	)

	switch query.Action {
	case "Latest":
		queryResult, queryErr = latest(h.Coll, query)
	case "Timestamp":
		queryResult, queryErr = timestamp(h.Coll, query)
	case "Items":
		queryResult, queryErr = items(h.Coll, query)
	case "OrQuery":
		queryResult, queryErr = orQuery(h.Coll, query)

	default:
		log.Printf("Unregistered Action found: %s", query.Action)
	}

	var (
		errCode int16
		errMsg  string
	)
	if queryErr != nil {
		errCode = queryErr.Code
		errMsg = queryErr.Message
	}

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating UUID for response")
		log.Println(err)
	}
	h.ResultProd <- &model.Document{
		CorrelationID: query.UUID,
		Data:          queryResult,
		Error:         errMsg,
		ErrorCode:     errCode,
		Source:        h.ServiceName,
		Topic:         query.ResponseTopic,
		UUID:          uuid,
	}
}
