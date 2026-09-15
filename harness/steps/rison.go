package steps

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const risonNotIDChar = " !\"#$%&'()*+,:;<=>?@[\\]^`{|}"

func risonEncode(v any) string {
	switch x := v.(type) {
	case nil:
		return "!n"
	case bool:
		if x {
			return "!t"
		}
		return "!f"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return risonString(x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = risonEncode(e)
		}
		return "!(" + strings.Join(parts, ",") + ")"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = risonString(k) + ":" + risonEncode(x[k])
		}
		return "(" + strings.Join(parts, ",") + ")"
	default:
		return risonString(fmt.Sprint(v))
	}
}

func risonString(s string) string {
	if risonIsIdent(s) {
		return s
	}
	r := strings.NewReplacer("!", "!!", "'", "!'")
	return "'" + r.Replace(s) + "'"
}

func risonIsIdent(s string) bool {
	if s == "" || strings.ContainsAny(s[:1], "-0123456789") {
		return false
	}
	return !strings.ContainsAny(s, risonNotIDChar)
}

// kueryFilter renders an exact-match filter map as a KQL expression.
func kueryFilter(filter map[string]string) string {
	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ":" + strconv.Quote(filter[k])
	}
	return strings.Join(parts, " and ")
}

func dashboardPNGJobParams(dashboardID, title, kuery string, since, until time.Time, width, height int, version string) string {
	params := map[string]any{
		"dashboardId":          dashboardID,
		"preserveSavedFilters": true,
		"timeRange": map[string]any{
			"from": since.UTC().Format(time.RFC3339),
			"to":   until.UTC().Format(time.RFC3339),
		},
		"useHash":  false,
		"viewMode": "view",
	}
	if kuery != "" {
		params["query"] = map[string]any{"language": "kuery", "query": kuery}
	}
	return risonEncode(map[string]any{
		"browserTimezone": "UTC",
		"layout": map[string]any{
			"id":         "preserve_layout",
			"dimensions": map[string]any{"width": width, "height": height},
		},
		"locatorParams": map[string]any{"id": "DASHBOARD_APP_LOCATOR", "params": params},
		"objectType":    "dashboard",
		"title":         title,
		"version":       version,
	})
}
