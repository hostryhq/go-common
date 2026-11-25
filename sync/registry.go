package sync

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Field policies (ADR-0002 S6).
const (
	PolicyImmutable = "immutable"
	PolicyLWW       = "lww"
	PolicyLWWReview = "lww_review"
	PolicyDelta     = "delta"
	PolicyRank      = "rank"
)

// UUIDv5Namespace is the namespace of the deterministic row ids (entities/README.md).
const UUIDv5Namespace = "9a3c7f1e-5b2d-5c84-a1e6-4d0f8b27c3a9"

// Field is one field of a synced entity.
type Field struct {
	Name     string
	Type     string
	Required bool
	Policy   string
	Group    string   // fields of one group win together (lww, lww_review)
	Order    []string // rank: values lowest first
	Values   []string // enum values
	Ref      string
	PII      bool
}

// Replica declares a read-only replica of an entity in another service (ADR-0014 A2.4).
type Replica struct {
	Service string
	Purpose string
}

// Entity is one synced entity from the api-specs entity registry.
type Entity struct {
	Name            string
	Version         int
	Owner           string
	Stream          string
	Policy          string // append, lww, rank or mixed
	Tombstone       bool
	Guarded         bool
	CloudOriginOnly bool
	IDRule          string // client_uuidv7 or uuidv5
	IDName          string // uuidv5 name template
	Fields          map[string]*Field
	ReplicatedTo    []Replica
}

// FieldNames returns the field names in a stable order.
func (e *Entity) FieldNames() []string {
	names := make([]string, 0, len(e.Fields))
	for n := range e.Fields {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// groupOf is the resolution group of an lww or lww_review field: its declared group, else itself.
func (f *Field) groupOf() string {
	if f.Group != "" {
		return "g:" + f.Group
	}
	return "f:" + f.Name
}

// ReplicatedTo reports whether service holds a read-only replica of e.
func (e *Entity) replicatedTo(service string) bool {
	for _, r := range e.ReplicatedTo {
		if r.Service == service {
			return true
		}
	}
	return false
}

// Registry is the set of synced entities.
type Registry struct {
	Entities map[string]*Entity
}

type entityDoc struct {
	Entity          string `yaml:"entity"`
	EntityV         int    `yaml:"entity_v"`
	Owner           string `yaml:"owner"`
	Stream          string `yaml:"stream"`
	Policy          string `yaml:"policy"`
	Tombstone       bool   `yaml:"tombstone"`
	Guarded         bool   `yaml:"guarded"`
	CloudOriginOnly bool   `yaml:"cloud_origin_only"`
	ID              struct {
		Rule string `yaml:"rule"`
		Name string `yaml:"name"`
	} `yaml:"id"`
	Fields map[string]struct {
		Type     string   `yaml:"type"`
		Required bool     `yaml:"required"`
		Policy   string   `yaml:"policy"`
		Group    string   `yaml:"group"`
		Order    []string `yaml:"order"`
		Values   []string `yaml:"values"`
		Ref      string   `yaml:"ref"`
		PII      bool     `yaml:"pii"`
	} `yaml:"fields"`
	ReplicatedTo []struct {
		Service string `yaml:"service"`
		Purpose string `yaml:"purpose"`
	} `yaml:"replicated_to"`
}

// LoadRegistry reads every *.yaml entity file under dir of fsys (the api-specs `entities`
// directory, one level of product folders). A service pins api-specs at a version and embeds or
// checks out that directory; the registry is the single source of the policies the engine applies.
func LoadRegistry(fsys fs.FS, dir string) (*Registry, error) {
	r := &Registry{Entities: map[string]*Entity{}}
	err := fs.WalkDir(fsys, dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path.Ext(p) != ".yaml" {
			return err
		}
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		e, err := parseEntity(b, strings.TrimSuffix(path.Base(p), ".yaml"))
		if err != nil {
			return fmt.Errorf("sync: %s: %w", p, err)
		}
		if _, dup := r.Entities[e.Name]; dup {
			return fmt.Errorf("sync: entity %s defined twice", e.Name)
		}
		r.Entities[e.Name] = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(r.Entities) == 0 {
		return nil, fmt.Errorf("sync: no entities under %s", dir)
	}
	return r, nil
}

func parseEntity(b []byte, fileName string) (*Entity, error) {
	var d entityDoc
	if err := yaml.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	if d.Entity != fileName {
		return nil, fmt.Errorf("entity %q does not equal the file name", d.Entity)
	}
	if d.Owner == "" {
		return nil, fmt.Errorf("owner is required")
	}
	e := &Entity{Name: d.Entity, Version: d.EntityV, Owner: d.Owner, Stream: d.Stream, Policy: d.Policy,
		Tombstone: d.Tombstone, Guarded: d.Guarded, CloudOriginOnly: d.CloudOriginOnly,
		IDRule: d.ID.Rule, IDName: d.ID.Name, Fields: map[string]*Field{}}
	for name, f := range d.Fields {
		switch f.Policy {
		case PolicyImmutable, PolicyLWW, PolicyLWWReview, PolicyDelta, PolicyRank:
		default:
			return nil, fmt.Errorf("field %s: policy %q", name, f.Policy)
		}
		if f.Policy == PolicyRank && len(f.Order) == 0 {
			return nil, fmt.Errorf("field %s: rank needs order", name)
		}
		e.Fields[name] = &Field{Name: name, Type: f.Type, Required: f.Required, Policy: f.Policy,
			Group: f.Group, Order: f.Order, Values: f.Values, Ref: f.Ref, PII: f.PII}
	}
	for _, rt := range d.ReplicatedTo {
		e.ReplicatedTo = append(e.ReplicatedTo, Replica{Service: rt.Service, Purpose: rt.Purpose})
	}
	if len(e.ReplicatedTo) > 0 {
		for _, f := range e.Fields {
			if f.PII {
				return nil, fmt.Errorf("a replicated entity must not have pii field %s", f.Name)
			}
		}
	}
	return e, nil
}

// Role is how a service holds an entity.
type Role int

// Roles of an entity for one service.
const (
	RoleNone    Role = iota // not ours: ignore its mutations
	RoleOwner               // owned: apply, and keep pii in the erasable side table
	RoleReplica             // declared read-only replica (replicated_to): apply, never keep pii
)

// RoleOf returns how service holds the named entity.
func (r *Registry) RoleOf(service, entity string) Role {
	e := r.Entities[entity]
	switch {
	case e == nil:
		return RoleNone
	case e.Owner == service:
		return RoleOwner
	case e.replicatedTo(service):
		return RoleReplica
	}
	return RoleNone
}

// Owned lists the entities service owns, sorted.
func (r *Registry) Owned(service string) []string { return r.names(service, RoleOwner) }

// Replicated lists the entities service holds as read-only replicas, sorted.
func (r *Registry) Replicated(service string) []string { return r.names(service, RoleReplica) }

func (r *Registry) names(service string, role Role) []string {
	var out []string
	for n := range r.Entities {
		if r.RoleOf(service, n) == role {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
