package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestProtocolCollector(t *testing.T) {
	previous := dhtStatsGetter
	t.Cleanup(func() { dhtStatsGetter = previous })
	SetDHTStatsGetter(func() DHTStats {
		return DHTStats{
			ProtocolEvents: []ProtocolEvent{{Stage: "metadata", Reason: "timeout", Count: 3}},
			QueueDepths:    map[string]int{"query": 2},
			ActiveWorkers:  map[string]int{"metadata": 1},
		}
	})
	collector := &protocolCollector{
		events:  prometheus.NewDesc("test_protocol_events_total", "Events", []string{"stage", "reason"}, nil),
		queues:  prometheus.NewDesc("test_queue_depth", "Queues", []string{"queue"}, nil),
		workers: prometheus.NewDesc("test_active_workers", "Workers", []string{"pool"}, nil),
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 3 {
		t.Fatalf("got %d metric families", len(families))
	}
	for _, family := range families {
		if len(family.Metric) != 1 {
			t.Fatalf("unexpected metric count for %s", family.GetName())
		}
		metric := family.Metric[0]
		switch family.GetName() {
		case "test_protocol_events_total":
			if metric.Counter.GetValue() != 3 || len(metric.Label) != 2 || metric.Label[0].GetValue() != "timeout" || metric.Label[1].GetValue() != "metadata" {
				t.Fatal("protocol outcome changed")
			}
		case "test_queue_depth":
			if metric.Gauge.GetValue() != 2 || metric.Label[0].GetValue() != "query" {
				t.Fatal("queue depth changed")
			}
		case "test_active_workers":
			if metric.Gauge.GetValue() != 1 || metric.Label[0].GetValue() != "metadata" {
				t.Fatal("worker count changed")
			}
		}
	}
	SetDHTStatsGetter(nil)
	families, err = registry.Gather()
	if err != nil || len(families) != 0 {
		t.Fatalf("collector without getter: %v %v", families, err)
	}
}
