// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

const (
	listRulesBenchNamespaces    = 50
	listRulesBenchGroupsPerNS   = 46
	listRulesBenchRulesPerGroup = 20
)

// listRulesBenchFixture builds one tenant with 50 namespaces, 2,300 groups and 46,000 rules.
// Even rules are recording rules.
// Odd rules are alerting rules.
func listRulesBenchFixture() rulespb.RuleGroupList {
	// %[1]d is the rule index.
	// %[2]d is a small per-rule number.
	templates := []struct {
		alert, record, expr string
	}{
		{
			alert:  "HighErrorRate",
			record: "job_instance:http_requests_errors:ratio_rate5m",
			expr:   `sum by (job, instance) (rate(http_requests_total{job="api-%[1]d", status=~"5.."}[5m])) / sum by (job, instance) (rate(http_requests_total{job="api-%[1]d"}[5m]))`,
		},
		{
			alert:  "HighRequestLatency",
			record: "job_route:http_request_duration_seconds:p99_rate5m",
			expr:   `histogram_quantile(0.99, sum by (le, job, route) (rate(http_request_duration_seconds_bucket{job="svc-%[1]d", cluster="prod-eu-west-%[2]d", route!~"/health.*|/ready"}[5m])))`,
		},
		{
			alert:  "NodeMemoryPressure",
			record: "cluster_instance:node_memory_utilisation:ratio",
			expr:   `(node_memory_MemTotal_bytes{cluster="prod-%[1]d", job="node-exporter"} - node_memory_MemAvailable_bytes{cluster="prod-%[1]d", job="node-exporter"}) / node_memory_MemTotal_bytes{cluster="prod-%[1]d", job="node-exporter"}`,
		},
		{
			alert:  "PodRestartingOften",
			record: "namespace_pod_owner:kube_pod_container_restarts:increase15m",
			expr:   `sum by (namespace, pod) (increase(kube_pod_container_status_restarts_total{namespace="team-%[1]d", container!="POD"}[15m])) * on (namespace, pod) group_left (owner_name) kube_pod_owner{namespace="team-%[1]d"}`,
		},
		{
			alert:  "WorkerCPUSaturation",
			record: "job:process_cpu_seconds:avg_rate5m_30m",
			expr:   `avg_over_time(sum without (instance) (rate(process_cpu_seconds_total{job="worker-%[1]d", env="production"}[5m]))[30m:1m]) / count without (instance) (up{job="worker-%[1]d", env="production"} == 1)`,
		},
	}

	intervals := []model.Duration{model.Duration(30 * time.Second), model.Duration(time.Minute), model.Duration(2 * time.Minute), model.Duration(5 * time.Minute)}
	forDurations := []model.Duration{model.Duration(time.Minute), model.Duration(5 * time.Minute), model.Duration(10 * time.Minute), model.Duration(15 * time.Minute)}

	list := make(rulespb.RuleGroupList, 0, listRulesBenchNamespaces*listRulesBenchGroupsPerNS)
	i := 0
	for ns := 0; ns < listRulesBenchNamespaces; ns++ {
		namespace := fmt.Sprintf("namespace-%02d", ns)
		for g := 0; g < listRulesBenchGroupsPerNS; g++ {
			rg := rulefmt.RuleGroup{
				Name:     fmt.Sprintf("group-%02d-%02d", ns, g),
				Interval: intervals[g%len(intervals)],
				Rules:    make([]rulefmt.Rule, 0, listRulesBenchRulesPerGroup),
			}
			for r := 0; r < listRulesBenchRulesPerGroup; r++ {
				tmpl := templates[i%len(templates)]
				expr := fmt.Sprintf(tmpl.expr, i, i%7)
				if i%2 == 0 {
					rg.Rules = append(rg.Rules, rulefmt.Rule{
						Record: fmt.Sprintf("%s_%d", tmpl.record, i),
						Expr:   expr,
					})
				} else {
					forDuration := forDurations[i%len(forDurations)]
					rule := rulefmt.Rule{
						Alert: fmt.Sprintf("%s%d", tmpl.alert, i),
						Expr:  fmt.Sprintf("%s > %d.%02d", expr, i%3, i%100),
						For:   forDuration,
						Labels: map[string]string{
							"severity": []string{"warning", "critical"}[i%4/2],
							"team":     fmt.Sprintf("team-%d", i%40),
						},
						Annotations: map[string]string{
							"summary": fmt.Sprintf("%s on {{ $labels.job }} is above threshold", tmpl.alert),
							"description": fmt.Sprintf("The value for {{ $labels.job }} on {{ $labels.instance }} is {{ $value | humanize }}.\n"+
								"This is above the threshold of %d.%02d for more than %s.\n"+
								"Check the service dashboard and recent deploys for team-%d.", i%3, i%100, forDuration, i%40),
						},
					}
					if i%4 == 1 {
						rule.Labels["service"] = fmt.Sprintf("service-%d", i%200)
						rule.Annotations["runbook_url"] = fmt.Sprintf("https://runbooks.example.com/alerts/%s#%d", tmpl.alert, i%50)
					}
					rg.Rules = append(rg.Rules, rule)
				}
				i++
			}
			list = append(list, rulespb.ToProto("user-1", namespace, rg))
		}
	}
	return list
}

func TestListRulesEncodeStreamingMatchesMarshal(t *testing.T) {
	rgs := listRulesBenchFixture()

	// Match marshalAndSend: output is an interface{} and Marshal gets a pointer to it.
	var output interface{} = rgs.Formatted()
	want, err := yaml.Marshal(&output)
	require.NoError(t, err)

	var got bytes.Buffer
	enc := yaml.NewEncoder(&got)
	require.NoError(t, enc.Encode(output))
	require.NoError(t, enc.Close())

	numRules, minExpr, maxExpr := 0, int(^uint(0)>>1), 0
	for _, g := range rgs {
		for _, r := range g.Rules {
			numRules++
			minExpr, maxExpr = min(minExpr, len(r.Expr)), max(maxExpr, len(r.Expr))
		}
	}
	t.Logf("groups=%d rules=%d expr_len=[%d,%d] marshal_bytes=%d encoder_bytes=%d", len(rgs), numRules, minExpr, maxExpr, len(want), got.Len())

	if !bytes.Equal(want, got.Bytes()) {
		n := min(len(want), got.Len())
		i := 0
		for i < n && want[i] == got.Bytes()[i] {
			i++
		}
		from := max(0, i-100)
		t.Fatalf("outputs differ at byte %d\nmarshal: %q\nencoder: %q", i, want[from:min(len(want), i+100)], got.Bytes()[from:min(got.Len(), i+100)])
	}
}

func BenchmarkListRulesEncode(b *testing.B) {
	rgs := listRulesBenchFixture()
	logger := log.NewNopLogger()

	var sizeOutput interface{} = rgs.Formatted()
	yamlBytes, err := yaml.Marshal(&sizeOutput)
	require.NoError(b, err)
	yamlSize := float64(len(yamlBytes))

	b.Run("formatted", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = rgs.Formatted()
		}
	})

	b.Run("yaml_marshal", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			d, err := yaml.Marshal(&output)
			if err != nil {
				b.Fatal(err)
			}
			n = len(d)
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	b.Run("marshalAndSend_recorder", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			rec := httptest.NewRecorder()
			marshalAndSend(rgs.Formatted(), rec, logger)
			n = rec.Body.Len()
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	b.Run("encoder_discard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			enc := yaml.NewEncoder(io.Discard)
			if err := enc.Encode(output); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(yamlSize, "out-bytes")
	})

	b.Run("encoder_bufio_discard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			bw := bufio.NewWriter(io.Discard)
			enc := yaml.NewEncoder(bw)
			if err := enc.Encode(output); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
			if err := bw.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(yamlSize, "out-bytes")
	})

	b.Run("encoder_recorder", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			rec := httptest.NewRecorder()
			enc := yaml.NewEncoder(rec)
			if err := enc.Encode(output); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
			n = rec.Body.Len()
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	// Reference floor only.
	// rulefmt has no json tags, so this output is not API compatible.
	b.Run("json_marshal", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			d, err := json.Marshal(output)
			if err != nil {
				b.Fatal(err)
			}
			n = len(d)
		}
		b.ReportMetric(float64(n), "out-bytes")
	})
}
