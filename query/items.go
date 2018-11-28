package query

import (
	"encoding/json"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type queryItems struct {
	Filter map[string]interface{} `json:"filter,omitempty"`
	Count  int64                  `json:"count,omitempty"`
}

func items(coll *mongo.Collection, query *cmodel.Command) ([]byte, *cmodel.Error) {
	qi := &queryItems{}
	err := json.Unmarshal(query.Data, &qi)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	if len(qi.Filter) == 0 {
		err = errors.New("blank filter provided")
		return nil, cmodel.NewError(cmodel.UserError, err.Error())
	}

	count := qi.Count
	if count < 1 {
		count = 50
	} else if count > 100 {
		count = 100
	}
	findopts := findopt.Limit(count)
	result, err := coll.Find(qi.Filter, findopts)
	if err != nil {
		err := errors.Wrap(err, "Error finding items from database")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Query: Error marshalling Inventory Delete-result")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	return resultMarshal, nil
}
