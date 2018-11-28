package query

import (
	"encoding/json"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type queryLatest struct {
	Count int64 `json:"count,omitempty"`
}

func latest(coll *mongo.Collection, query *cmodel.Command) ([]byte, *cmodel.Error) {
	latest := &queryLatest{}
	err := json.Unmarshal(query.Data, latest)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling query-data to latest")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	params := map[string]interface{}{
		"timestamp": map[string]interface{}{
			"$ne": 0,
		},
	}
	count := latest.Count
	if count < 1 {
		count = 50
	} else if count > 100 {
		count = 100
	}
	findopts := findopt.Limit(count)
	result, err := coll.Find(params, findopts)
	if err != nil {
		err = errors.Wrap(err, "Error getting items from database")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	marshalItems, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling items")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	return marshalItems, nil
}
