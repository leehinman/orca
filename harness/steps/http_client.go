package steps

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"github.com/elastic/orca/harness"
)

func buildTLSTransport(state *harness.State, caPath string) (*http.Transport, error) {
	resolved, err := state.Resolve(caPath)
	if err != nil {
		return nil, fmt.Errorf("resolve CA path: %w", err)
	}
	caCert, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %q: %w", resolved, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("parse CA cert %q", resolved)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	return transport, nil
}
