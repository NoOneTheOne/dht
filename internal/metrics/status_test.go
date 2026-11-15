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

func TestFileCoverage(t *testing.T) {
	for _, tc := range []struct{ total, with int64 }{{0, 0}, {4, 1}, {10, 0}, {10, 10}} {
		coverage := NewFileCoverage(tc.total, tc.with)
		if coverage.Total != tc.total || coverage.WithFiles != tc.with || coverage.WithoutFiles != tc.total-tc.with {
			t.Fatalf("incorrect counts: %+v", coverage)
		}
		if tc.total == 0 {
			if coverage.WithFilesPercent != nil || coverage.WithoutFilesPercent != nil {
				t.Fatal("empty coverage must have unknown percentages")
			}
		} else if coverage.WithFilesPercent == nil || coverage.WithoutFilesPercent == nil ||
			*coverage.WithFilesPercent != float64(tc.with)/float64(tc.total)*100 ||
			*coverage.WithoutFilesPercent != float64(tc.total-tc.with)/float64(tc.total)*100 {
			t.Fatalf("incorrect percentages: %+v", coverage)
		}
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
