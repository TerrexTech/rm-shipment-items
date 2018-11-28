package query

import (
	"encoding/json"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/pkg/errors"
)

type orQueryParams struct {
	Filter []map[string]interface{} `json:"filter,omitempty"`
	Count  int64                    `json:"count,omitempty"`
}

func orQuery(coll *mongo.Collection, query *cmodel.Command) ([]byte, *cmodel.Error) {
	oq := &orQueryParams{}
	err := json.Unmarshal(query.Data, oq)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	if len(oq.Filter) == 0 {
		err = errors.New("blank filter provided")
		return nil, cmodel.NewError(cmodel.UserError, err.Error())
	}

	count := oq.Count
	if count < 1 {
		count = 50
	} else if count > 100 {
		count = 100
	}
	findopts := findopt.Limit(count)

	filter := map[string]interface{}{
		"$or": oq.Filter,
	}
	result, err := coll.Find(filter, findopts)
	if err != nil {
		err := errors.Wrap(err, "Error finding items in database")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Query: Error marshalling Inventory Delete-result")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	return resultMarshal, nil
}
