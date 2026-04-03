package schema

import (
	"fmt"
	"sync"
)

// Index defines a secondary index on a set of columns.
type Index struct {
	Columns []string `json:"columns"`
}

// TableSchema defines the schema for a table including columns, sinks, and hints.
type TableSchema struct {
	Table                string                        `json:"table"`
	Columns              map[string]ColumnType          `json:"columns"`
	Sinks                []string                       `json:"sinks,omitempty"`
	PrimaryKey           []string                       `json:"primary_key,omitempty"`
	Indexes              []Index                        `json:"indexes,omitempty"`
	PartitionBy          string                         `json:"partition_by,omitempty"`
	DesignatedTimestamp   string                         `json:"designated_timestamp,omitempty"`
	Overrides            map[string]map[string]string   `json:"overrides,omitempty"`
}

// ColumnTypeFor returns the native type for a column in a specific sink,
// checking overrides first, then falling back to the portable type mapping.
func (s *TableSchema) ColumnTypeFor(col string, sinkName string) string {
	if overrides, ok := s.Overrides[sinkName]; ok {
		if native, ok := overrides[col]; ok {
			return native
		}
	}
	colType, ok := s.Columns[col]
	if !ok {
		return "TEXT"
	}
	return NativeType(colType, sinkName)
}

// Validate checks the schema for correctness.
func (s *TableSchema) Validate(enabledSinks []string) error {
	if s.Table == "" {
		return fmt.Errorf("table name is required")
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("at least one column is required")
	}
	for name, ct := range s.Columns {
		if err := ValidateColumnType(ct); err != nil {
			return fmt.Errorf("column %q: %w", name, err)
		}
	}
	for _, pk := range s.PrimaryKey {
		if _, ok := s.Columns[pk]; !ok {
			return fmt.Errorf("primary_key column %q not found in columns", pk)
		}
	}
	for i, idx := range s.Indexes {
		if len(idx.Columns) == 0 {
			return fmt.Errorf("index %d has no columns", i)
		}
		for _, col := range idx.Columns {
			if _, ok := s.Columns[col]; !ok {
				return fmt.Errorf("index %d references unknown column %q", i, col)
			}
		}
	}
	if s.PartitionBy != "" {
		if _, ok := s.Columns[s.PartitionBy]; !ok {
			return fmt.Errorf("partition_by column %q not found in columns", s.PartitionBy)
		}
	}
	if s.DesignatedTimestamp != "" {
		if _, ok := s.Columns[s.DesignatedTimestamp]; !ok {
			return fmt.Errorf("designated_timestamp column %q not found in columns", s.DesignatedTimestamp)
		}
	}
	// Validate sink names reference enabled sinks
	enabledSet := make(map[string]bool, len(enabledSinks))
	for _, name := range enabledSinks {
		enabledSet[name] = true
	}
	for _, sinkName := range s.Sinks {
		if !enabledSet[sinkName] {
			return fmt.Errorf("sink %q is not enabled in config", sinkName)
		}
	}
	return nil
}

// Registry holds all registered table schemas in memory.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string]*TableSchema
	// Names of all enabled sinks, used as default when schema.Sinks is empty.
	enabledSinks []string
}

func NewRegistry(enabledSinks []string) *Registry {
	return &Registry{
		schemas:      make(map[string]*TableSchema),
		enabledSinks: enabledSinks,
	}
}

func (r *Registry) Register(s *TableSchema) error {
	if err := s.Validate(r.enabledSinks); err != nil {
		return err
	}
	// Default to all enabled sinks if none specified.
	if len(s.Sinks) == 0 {
		s.Sinks = make([]string, len(r.enabledSinks))
		copy(s.Sinks, r.enabledSinks)
	}
	r.mu.Lock()
	r.schemas[s.Table] = s
	r.mu.Unlock()
	return nil
}

func (r *Registry) Get(table string) (*TableSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[table]
	return s, ok
}

func (r *Registry) SinksFor(table string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[table]
	if !ok {
		return nil
	}
	return s.Sinks
}

func (r *Registry) List() []*TableSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*TableSchema, 0, len(r.schemas))
	for _, s := range r.schemas {
		result = append(result, s)
	}
	return result
}
