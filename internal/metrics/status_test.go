package metrics

import "testing"

func TestOutcomeRate(t *testing.T) {
	events := []ProtocolEvent{{Stage: "query", Reason: "success", Count: 3}, {Stage: "query", Reason: "timeout", Count: 1}, {Stage: "query", Reason: "stopped", Count: 10}, {Stage: "metadata", Reason: "success", Count: 8}}
	r := OutcomeRate(events, "query")
	if r.Succeeded != 3 || r.Failed != 1 || r.SuccessRate == nil || *r.SuccessRate != 75 {
		t.Fatalf("unexpected rate: %+v", r)
	}
	if r := OutcomeRate(nil, "metadata"); r.SuccessRate != nil {
		t.Fatal("empty sample must not report zero success")
	}
}

func TestSystemStatus(t *testing.T) {
	total := int64(10)
	for _, tc := range []struct {
		name    string
		crawler *CrawlerStatus
		total   *int64
		want    string
	}{
		{"healthy", &CrawlerStatus{Ready: true, Reachability: "reachable", NAT: "manual"}, &total, "operational"},
		{"unknown ingress", &CrawlerStatus{Ready: true, Reachability: "unknown", NAT: "manual"}, &total, "degraded"},
		{"mapping failure", &CrawlerStatus{Ready: true, Reachability: "reachable", NAT: "mapping_failed"}, &total, "degraded"},
		{"query failure", &CrawlerStatus{Ready: true, Reachability: "reachable", NAT: "manual", DHTQuery: SuccessRate{Failed: 20}}, &total, "degraded"},
		{"crawler down", nil, &total, "outage"},
		{"storage down", &CrawlerStatus{Ready: true, Reachability: "reachable"}, nil, "outage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSystemStatus(tc.crawler, tc.total)
			if s.Status != tc.want || s.Issues == nil {
				t.Fatalf("unexpected status: %+v", s)
			}
		})
	}
}
