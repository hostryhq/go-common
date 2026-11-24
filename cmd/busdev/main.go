// Command busdev generates dev-only NATS nkeys and the permissions config from the
// api-specs event schemas (ADR-0011 B8). Seeds are written under <out>/seeds.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hostryhq/go-common/bus"
)

func main() {
	events := flag.String("events", "../api-specs/events", "api-specs events directory")
	out := flag.String("out", ".nats", "output directory (accounts.conf, seeds/)")
	flag.Parse()
	reg, err := bus.LoadRegistry(os.DirFS(*events))
	if err != nil {
		fail(err)
	}
	keys, err := bus.GenerateKeys(reg.Services())
	if err != nil {
		fail(err)
	}
	if err := reg.WriteDevFiles(*out, keys); err != nil {
		fail(err)
	}
	fmt.Printf("wrote %s/accounts.conf for services %v\n", *out, reg.Services())
}

func fail(err error) { fmt.Fprintln(os.Stderr, "busdev:", err); os.Exit(1) }
