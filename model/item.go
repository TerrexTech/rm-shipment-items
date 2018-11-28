package model

// AggregateID for Shipment aggregate.
const AggregateID = 2

// Item defines the an item in Shipment.
type Item struct {
	ItemID          string  `bson:"itemID,omitempty" json:"itemID,omitempty"`
	DateArrived     int64   `bson:"dateArrived,omitempty" json:"dateArrived,omitempty"`
	DisposedWeight  float64 `bson:"disposedWeight,omitempty" json:"disposedWeight,omitempty"`
	DonatedWeight   float64 `bson:"donatedWeight,omitempty" json:"donatedWeight,omitempty"`
	FlashSoldWeight float64 `bson:"flashSoldWeight,omitempty" json:"flashSoldWeight,omitempty"`
	Lot             string  `bson:"lot,omitempty" json:"lot,omitempty"`
	Name            string  `bson:"name,omitempty" json:"name,omitempty"`
	Origin          string  `bson:"origin,omitempty" json:"origin,omitempty"`
	Price           float64 `bson:"price,omitempty" json:"price,omitempty"`
	RSCustomerID    string  `bson:"rsCustomerID,omitempty" json:"rsCustomerID,omitempty"`
	SKU             string  `bson:"sku,omitempty" json:"sku,omitempty"`
	SoldWeight      float64 `bson:"soldWeight,omitempty" json:"soldWeight,omitempty"`
	Timestamp       int64   `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
	TotalWeight     float64 `bson:"totalWeight,omitempty" json:"totalWeight,omitempty"`
	UPC             string  `bson:"upc,omitempty" json:"upc,omitempty"`
}
