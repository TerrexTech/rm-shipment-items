package query

import (
	"encoding/json"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rm-shipment-items/model"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/pkg/errors"
)

type queryTimestamp struct {
	Start int64 `json:"start,omitempty"`
	End   int64 `json:"end,omitempty"`
	Count int64 `json:"count,omitempty"`
}

func timestamp(coll *mongo.Collection, query *cmodel.Command) ([]byte, *cmodel.Error) {
	tq := &queryTimestamp{}
	err := json.Unmarshal(query.Data, tq)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling queryTimestamp")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	if tq.Start == 0 && tq.End == 0 {
		err := errors.New("either start-time or end-time must be non-zero")
		return nil, cmodel.NewError(cmodel.UserError, err.Error())
	}

	count := tq.Count
	if count < 1 {
		count = 50
	} else if count > 100 {
		count = 100
	}

	params := map[string]interface{}{}
	if tq.Start != 0 {
		params["$gt"] = tq.Start
	}
	if tq.End != 0 {
		params["$lt"] = tq.End
	}
	dbQuery := map[string]interface{}{
		"timestamp": params,
	}
	findopts := findopt.Limit(count)
	results, err := coll.Find(dbQuery, findopts)
	if err != nil {
		err = errors.Wrap(err, "Error finding data in database")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	items := []model.Item{}
	for _, resultItem := range results {
		item, assertOK := resultItem.(*model.Item)
		if !assertOK {
			err := errors.New("Error asserting resultItem")
			return nil, cmodel.NewError(cmodel.InternalError, err.Error())
		}
		items = append(items, *item)
	}

	marshalItems, err := json.Marshal(items)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling items")
		return nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	return marshalItems, nil
}
