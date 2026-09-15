package steps

import (
	"fmt"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/orca/harness"
)

func buildESClient(state *harness.State, cfg harness.ESConfig) (*elasticsearch.Client, error) {
	url, err := state.Resolve(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("resolve ES URL: %w", err)
	}
	if url == "" {
		return nil, fmt.Errorf("elasticsearch.url is not configured")
	}
	user, _ := state.Resolve(cfg.Username)
	pass, _ := state.Resolve(cfg.Password)

	esCfg := elasticsearch.Config{
		Addresses: []string{url},
		Username:  user,
		Password:  pass,
	}

	if cfg.CAPath != "" {
		transport, err := buildTLSTransport(state, cfg.CAPath)
		if err != nil {
			return nil, err
		}
		esCfg.Transport = transport
	}

	return elasticsearch.NewClient(esCfg)
}
