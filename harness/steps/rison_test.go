package steps

import (
	"testing"
	"time"
)

func TestRisonEncode(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"ident", "preserve_layout", "preserve_layout"},
		{"quoted with colon", "agent.name:x", "'agent.name:x'"},
		{"leading digit", "9.5.4", "'9.5.4'"},
		{"escapes", "it's !", "'it!'s !!'"},
		{"bool", true, "!t"},
		{"int", 1600, "1600"},
		{"nil", nil, "!n"},
		{"array", []any{"a", 1, false}, "!(a,1,!f)"},
		{"sorted map", map[string]any{"b": 1, "a": map[string]any{"z": "y"}}, "(a:(z:y),b:1)"},
		{"double quotes unescaped", `agent.name:"bench-0"`, `'agent.name:"bench-0"'`},
	}
	for _, c := range cases {
		if got := risonEncode(c.in); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestKueryFilter(t *testing.T) {
	got := kueryFilter(map[string]string{"host.name": "h1", "agent.name": "bench-elastic-agent-0"})
	want := `agent.name:"bench-elastic-agent-0" and host.name:"h1"`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if kueryFilter(nil) != "" {
		t.Error("empty filter should render empty kuery")
	}
}

func TestDashboardPNGJobParams(t *testing.T) {
	since := time.Date(2026, 9, 30, 20, 10, 0, 0, time.UTC)
	until := since.Add(15 * time.Minute)
	got := dashboardPNGJobParams("dash-1", "bench agent metrics", `agent.name:"bench-0"`, since, until, 1600, 2400, "9.5.4")
	want := `(browserTimezone:UTC,layout:(dimensions:(height:2400,width:1600),id:preserve_layout),` +
		`locatorParams:(id:DASHBOARD_APP_LOCATOR,params:(dashboardId:dash-1,preserveSavedFilters:!t,` +
		`query:(language:kuery,query:'agent.name:"bench-0"'),` +
		`timeRange:(from:'2026-09-30T20:10:00Z',to:'2026-09-30T20:25:00Z'),useHash:!f,viewMode:view)),` +
		`objectType:dashboard,title:'bench agent metrics',version:'9.5.4')`
	if got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
	noQuery := dashboardPNGJobParams("dash-1", "t", "", since, until, 1, 1, "v")
	if contains(noQuery, "query:(") {
		t.Errorf("empty kuery should omit query: %s", noQuery)
	}
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
