package main

import (
	"flag"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/app"
)

func main() {
	standalone := flag.Bool("standalone", false, "If true, do not auto-spawn missing worker processes (connect only to external worker processes)")
	flag.Parse()

	app.RunGateway(!*standalone)
}
