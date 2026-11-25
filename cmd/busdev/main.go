// Command busdev generates dev-only NATS nkeys and the permissions config from the
// api-specs event schemas (ADR-0011 B8). Seeds are written under <out>/seeds.
//
// With -provision <nats-url> it instead creates the streams and durable consumers on a
// running dev server with the admin seed already in <out>/seeds (keys are not regenerated).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hostryhq/go-common/bus"
)

func main() {
	events := flag.String("events", "../api-specs/events", "api-specs events directory")
	out := flag.String("out", ".nats", "output directory (accounts.conf, seeds/)")
	provision := flag.String("provision", "", "NATS URL: create streams and consumers with <out>/seeds/admin.nk, do not generate keys (dev only)")
	flag.Parse()
	reg, err := bus.LoadRegistry(os.DirFS(*events))
	if err != nil {
		fail(err)
	}
	if *provision != "" {
		if err := provisionStreams(*provision, filepath.Join(*out, "seeds", bus.AdminUser+".nk"), reg); err != nil {
			fail(err)
		}
		fmt.Printf("provisioned streams and consumers on %s\n", *provision)
		return
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

// provisionStreams runs bus.Provision as the admin user against a running dev NATS.
func provisionStreams(url, seedFile string, reg *bus.Registry) error {
	nc, err := bus.ConnectAdmin(url, seedFile)
	if err != nil {
		return err
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 256 MiB per stream: every publisher's stream must fit the dev server's 4 GB JetStream store.
	return bus.Provision(ctx, nc, reg, bus.ProvisionOptions{Generation: "dev-1", Epoch: "genesis", MaxBytes: 256 << 20})
}

func fail(err error) { fmt.Fprintln(os.Stderr, "busdev:", err); os.Exit(1) }
