// Command vgi-loki-worker serves the loki catalog to DuckDB.
//
//	vgi-loki-worker                                  # stdio (what DuckDB launches)
//	vgi-loki-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-loki-worker --unix /tmp/loki.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'loki' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-loki-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-loki/internal/loki"
	"github.com/Query-farm/vgi-loki/internal/serve"
)

func main() {
	serve.Main("vgi-loki-worker", os.Args[1:], func() *vgi.Worker { return loki.NewWorker() })
}
