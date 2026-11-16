package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMiddlewareBoundsMethodLabels(t *testing.T) {
	router := gin.New()
	router.Use(Middleware())
	router.NoRoute(func(c *gin.Context) { c.String(http.StatusNotFound, "missing") })
	for i := 0; i < 100; i++ {
		request := httptest.NewRequest(fmt.Sprintf("CUSTOM%d", i), "/metrics-method-test", strings.NewReader("body"))
		router.ServeHTTP(httptest.NewRecorder(), request)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(httpRequestsTotal, httpRequestDuration, httpRequestSize, httpResponseSize)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 4 {
		t.Fatalf("got %d metric families, want 4", len(families))
	}
	for _, family := range families {
		count := 0
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "method" {
					if strings.HasPrefix(label.GetValue(), "CUSTOM") {
						t.Fatalf("unbounded method label: %s", label.GetValue())
					}
					if label.GetValue() == "OTHER" {
						count++
					}
				}
			}
		}
		if count != 1 {
			t.Fatalf("%s: got %d OTHER metrics, want 1", family.GetName(), count)
		}
	}
}

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
