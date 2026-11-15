package metrics

import "time"

type SuccessRate struct {
	Succeeded   int64    `json:"succeeded"`
	Failed      int64    `json:"failed"`
	SuccessRate *float64 `json:"success_rate"`
}

func OutcomeRate(events []ProtocolEvent, stage string) SuccessRate {
	var rate SuccessRate
	for _, event := range events {
		if event.Stage != stage || event.Reason == "stopped" {
			continue
		}
		if event.Reason == "success" {
			rate.Succeeded += event.Count
		} else {
			rate.Failed += event.Count
		}
	}
	if total := rate.Succeeded + rate.Failed; total > 0 {
		value := float64(rate.Succeeded) / float64(total) * 100
		rate.SuccessRate = &value
	}
	return rate
}

type CrawlerStatus struct {
	StartedAt      time.Time      `json:"started_at"`
	UptimeSeconds  int64          `json:"uptime_seconds"`
	Collected      int64          `json:"collected"`
	ConnectedPeers int            `json:"connected_peers"`
	RejectedPeers  int64          `json:"rejected_peers"`
	DroppedPeers   int64          `json:"dropped_peers"`
	Workers        map[string]int `json:"workers"`
	WorkerLimits   map[string]int `json:"worker_limits"`
	DHTQuery       SuccessRate    `json:"dht_query"`
	MetadataFetch  SuccessRate    `json:"metadata_fetch"`
	NAT            string         `json:"nat"`
	Reachability   string         `json:"reachability"`
	Ready          bool           `json:"ready"`
}

type SystemStatus struct {
	Status         string         `json:"status"`
	Issues         []string       `json:"issues"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Crawler        *CrawlerStatus `json:"crawler"`
	TotalCollected *int64         `json:"total_collected"`
	Storage        string         `json:"storage"`
}

func NewSystemStatus(crawler *CrawlerStatus, total *int64) SystemStatus {
	s := SystemStatus{Status: "operational", Issues: []string{}, UpdatedAt: time.Now().UTC(), Crawler: crawler, TotalCollected: total, Storage: "connected"}
	if crawler == nil {
		s.Issues = append(s.Issues, "采集进程暂时无法连接")
	} else {
		if !crawler.Ready {
			s.Issues = append(s.Issues, "DHT 网络正在初始化")
		}
		if crawler.Reachability != "reachable" {
			s.Issues = append(s.Issues, "公网 UDP 入站可达性尚未确认")
		}
		if crawler.NAT == "mapping_failed" || crawler.NAT == "unavailable" {
			s.Issues = append(s.Issues, "自动 NAT 映射未建立，请检查手动转发")
		}
		if crawler.DHTQuery.Failed >= 20 && crawler.DHTQuery.Succeeded == 0 {
			s.Issues = append(s.Issues, "DHT 查询尚无成功响应")
		}
		if crawler.MetadataFetch.Failed >= 20 && crawler.MetadataFetch.Succeeded == 0 {
			s.Issues = append(s.Issues, "元数据抓取尚无成功结果")
		}
	}
	if total == nil {
		s.Storage = "unavailable"
		s.Issues = append(s.Issues, "资源数据库暂时不可用")
	}
	if len(s.Issues) > 0 {
		s.Status = "degraded"
	}
	if total == nil || crawler == nil {
		s.Status = "outage"
	}
	return s
}
