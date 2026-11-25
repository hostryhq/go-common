// Package bus is the NATS JetStream side of ADR-0011: the schema registry that
// drives permissions and consumers, stream/consumer provisioning, the publisher
// used by the outbox relay, and the durable pull consumer and signal listener
// used by subscribers. Only this package imports nats.go.
package bus

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// EventType is one event schema from api-specs/events.
type EventType struct {
	// Type is "<service>.<entity>.<verb>".
	Type string
	// Major is the schema major version.
	Major int
	// Publisher is the x-publisher service.
	Publisher string
	// Subscribers are the x-subscribers services (durable consumers).
	Subscribers []string
	// SignalListeners are the optional x-signal-listeners services (plain subscriptions).
	SignalListeners []string
	// SubscribersByNamespace is set only for the sync op family (type "sync.op", ADR-0014):
	// the real subject is evt.sync.op.<namespace>.v1 and a subscriber consumes only
	// the namespaces listed for it.
	SubscribersByNamespace map[string][]string
}

// SyncOpType is the one two-segment event type: the sync gateway appends the
// namespace at publish time (sync.op.<namespace>).
const SyncOpType = "sync.op"

// Subject is evt.<type>.v<major>.
// For the sync op family the namespace is a wildcard token: evt.sync.op.*.v1.
func (t EventType) Subject() string {
	if t.Type == SyncOpType {
		return fmt.Sprintf("evt.%s.*.v%d", t.Type, t.Major)
	}
	return fmt.Sprintf("evt.%s.v%d", t.Type, t.Major)
}

// Registry is the set of event types, the source of generated permissions and consumers.
type Registry struct{ Types []EventType }

var idRe = regexp.MustCompile(`^urn:hostry:events:([a-z]+\.[a-z_]+\.[a-z_]+|sync\.op):v([0-9]+)$`)

var nsRe = regexp.MustCompile(`^[a-z]+$`)

// LoadRegistry reads every <service>/<type>.v<N>.yaml under fsys (the api-specs
// events directory). Files starting with "_" (the envelope) are skipped.
func LoadRegistry(fsys fs.FS) (*Registry, error) {
	var r Registry
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".yaml") || strings.HasPrefix(path.Base(p), "_") {
			return err
		}
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		var doc struct {
			ID              string              `yaml:"$id"`
			Publisher       string              `yaml:"x-publisher"`
			Subscribers     []string            `yaml:"x-subscribers"`
			SignalListeners []string            `yaml:"x-signal-listeners"`
			ByNamespace     map[string][]string `yaml:"x-subscribers-by-namespace"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return fmt.Errorf("bus: %s: %w", p, err)
		}
		m := idRe.FindStringSubmatch(doc.ID)
		if m == nil || doc.Publisher == "" {
			return fmt.Errorf("bus: %s: need $id urn:hostry:events:<type>:v<N> and x-publisher", p)
		}
		major, _ := strconv.Atoi(m[2])
		if !strings.HasPrefix(m[1], doc.Publisher+".") {
			return fmt.Errorf("bus: %s: type %s is not prefixed by publisher %s", p, m[1], doc.Publisher)
		}
		if m[1] == SyncOpType && doc.Publisher != "sync" {
			return fmt.Errorf("bus: %s: type %s is published only by sync", p, m[1])
		}
		if m[1] != SyncOpType && len(doc.ByNamespace) > 0 {
			return fmt.Errorf("bus: %s: x-subscribers-by-namespace is only for %s", p, SyncOpType)
		}
		for ns, svcs := range doc.ByNamespace {
			if !nsRe.MatchString(ns) || ns == "flagged" {
				// "flagged" is the gateway event sync.op.flagged, not an entity namespace.
				return fmt.Errorf("bus: %s: bad namespace %q", p, ns)
			}
			for _, svc := range svcs {
				if !slices.Contains(doc.Subscribers, svc) {
					return fmt.Errorf("bus: %s: namespace %s subscriber %s not in x-subscribers", p, ns, svc)
				}
			}
		}
		r.Types = append(r.Types, EventType{Type: m[1], Major: major, Publisher: doc.Publisher,
			Subscribers: doc.Subscribers, SignalListeners: doc.SignalListeners, SubscribersByNamespace: doc.ByNamespace})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(r.Types, func(i, j int) bool { return r.Types[i].Subject() < r.Types[j].Subject() })
	return &r, nil
}

// Services returns every service named as publisher, subscriber or signal listener, sorted.
func (r *Registry) Services() []string {
	set := map[string]bool{}
	for _, t := range r.Types {
		set[t.Publisher] = true
		for _, s := range append(append([]string{}, t.Subscribers...), t.SignalListeners...) {
			set[s] = true
		}
	}
	return sortedKeys(set)
}

// Publishers returns the services that publish at least one type, sorted.
func (r *Registry) Publishers() []string {
	set := map[string]bool{}
	for _, t := range r.Types {
		set[t.Publisher] = true
	}
	return sortedKeys(set)
}

// Consumption is one durable consumer: service Consumer reads Stream, filtered to Subjects.
type Consumption struct {
	Consumer string
	Stream   string
	Subjects []string
}

// Consumptions returns the durable consumers implied by x-subscribers: one per
// consuming service per publishing stream.
func (r *Registry) Consumptions() []Consumption {
	byKey := map[string]*Consumption{}
	var keys []string
	for _, t := range r.Types {
		for _, s := range t.Subscribers {
			subjects := []string{t.Subject()}
			if len(t.SubscribersByNamespace) > 0 {
				subjects = nil
				for _, ns := range sortedNamespaces(t.SubscribersByNamespace) {
					if slices.Contains(t.SubscribersByNamespace[ns], s) {
						subjects = append(subjects, fmt.Sprintf("evt.%s.%s.v%d", t.Type, ns, t.Major))
					}
				}
				if len(subjects) == 0 {
					continue
				}
			}
			k := StreamName(t.Publisher) + "/" + s
			c := byKey[k]
			if c == nil {
				c = &Consumption{Consumer: s, Stream: StreamName(t.Publisher)}
				byKey[k] = c
				keys = append(keys, k)
			}
			c.Subjects = append(c.Subjects, subjects...)
		}
	}
	sort.Strings(keys)
	out := make([]Consumption, 0, len(keys))
	for _, k := range keys {
		out = append(out, *byKey[k])
	}
	return out
}

// StreamName is EVT_<SERVICE> for a publishing service.
func StreamName(service string) string { return "EVT_" + strings.ToUpper(service) }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedNamespaces(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
