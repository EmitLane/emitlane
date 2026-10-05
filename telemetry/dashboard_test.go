package telemetry

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Catch metric renames and misspellings before shipping an empty dashboard.
func TestDashboardQueriesReferenceExposedMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.SetConsumerLag("test", "orders", 0)
	metrics.AddConsumerPaused("test", "retry", 0)
	metrics.ObserveConsumerRecord("test", "processed", 0)
	metrics.IncConsumerDuplicate("test")
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	known := make(map[string]bool)
	for _, family := range families {
		name := family.GetName()
		known[name] = true
		if family.GetType().String() == "HISTOGRAM" {
			known[name+"_bucket"] = true
			known[name+"_sum"] = true
			known[name+"_count"] = true
		}
	}
	contents, err := os.ReadFile("../deploy/monitoring/grafana-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Panels []struct {
			Title   string
			Targets []struct{ Expr string }
		}
	}
	if err := json.Unmarshal(contents, &dashboard); err != nil {
		t.Fatal(err)
	}
	metricName := regexp.MustCompile(`\bemitlane_[a-z_]+\b`)
	nameSelector := regexp.MustCompile(`__name__=~"([^"]+)"`)
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			expression := target.Expr
			for _, selector := range nameSelector.FindAllStringSubmatch(expression, -1) {
				pattern, err := regexp.Compile("^(?:" + selector[1] + ")$")
				if err != nil {
					t.Fatal(err)
				}
				matches := 0
				for name := range known {
					if pattern.MatchString(name) {
						matches++
					}
				}
				if matches == 0 {
					t.Errorf("panel %q selects no exposed metrics: %s", panel.Title, selector[1])
				}
			}
			expression = nameSelector.ReplaceAllString(expression, "")
			for _, name := range metricName.FindAllString(expression, -1) {
				if !known[name] {
					t.Errorf("panel %q references missing metric %s", panel.Title, name)
				}
			}
		}
	}
}
