package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	totalRecordLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "xjoin_total_record_lag",
		Help: "The number of milliseconds between debezium reading a record and elasticsearch indexing the record.",
	})

	debeziumLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "xjoin_debezium_lag",
		Help: "The number of milliseconds between debezium reading a record from the database and writing the record to the source topic.",
	})

	coreLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "xjoin_core_lag",
		Help: "The number of milliseconds between xjoin-core reading from the source topic and writing to the sink topic.",
	})
)

func ObserveTotalRecordLag(lag float64) {
	totalRecordLag.Set(lag)
}

func ObserveDebeziumLag(lag float64) {
	debeziumLag.Set(lag)
}

func ObserveCoreLag(lag float64) {
	coreLag.Set(lag)
}

func Push(url string, job string) error {
	return push.New(url, job).
		Collector(totalRecordLag).
		Collector(debeziumLag).
		Collector(coreLag).
		Push()
}
