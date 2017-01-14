package router

type RangeName struct {
	Low, High uint64
	Parents   []RangeName `json:"-"`
}
