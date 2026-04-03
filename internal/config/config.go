package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig          `yaml:"server"`
	Metrics MetricsConfig         `yaml:"metrics"`
	Batcher BatcherConfig         `yaml:"batcher"`
	Sinks   map[string]SinkConfig `yaml:"sinks"`
	Trino   TrinoConfig           `yaml:"trino"`
	MCP     MCPConfig             `yaml:"mcp"`
}

type TrinoConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
}

type MCPConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Transport string `yaml:"transport"`  // "stdio" or "http"
	HTTPPort  int    `yaml:"http_port"`  // only used when transport is "http"
}

type ServerConfig struct {
	Bind string `yaml:"bind"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Bind    string `yaml:"bind"`
}

type BatcherConfig struct {
	MaxSize     int           `yaml:"max_size"`
	FlushPeriod time.Duration `yaml:"flush_period"`
	BufferSize  int           `yaml:"buffer_size"`
}

type SinkConfig struct {
	Enabled     bool   `yaml:"enabled"`
	DSN         string `yaml:"dsn"`
	PasswordEnv string `yaml:"password_env"`
	BulkSize    int    `yaml:"bulk_size"`
	MaxRetries  int    `yaml:"max_retries"`
	RetryBaseMS int    `yaml:"retry_base_ms"`
	MaxConns    int    `yaml:"max_conns"`
	TimeoutSecs int    `yaml:"timeout_secs"`
}

func (c *Config) setDefaults() {
	if c.Server.Bind == "" {
		c.Server.Bind = "0.0.0.0:8080"
	}
	if c.Metrics.Bind == "" {
		c.Metrics.Bind = "0.0.0.0:9090"
	}
	if c.Batcher.MaxSize == 0 {
		c.Batcher.MaxSize = 5000
	}
	if c.Batcher.FlushPeriod == 0 {
		c.Batcher.FlushPeriod = time.Second
	}
	if c.Batcher.BufferSize == 0 {
		c.Batcher.BufferSize = 100000
	}
	for name, s := range c.Sinks {
		if s.BulkSize == 0 {
			s.BulkSize = 5000
		}
		if s.MaxRetries == 0 {
			s.MaxRetries = 3
		}
		if s.RetryBaseMS == 0 {
			s.RetryBaseMS = 100
		}
		if s.MaxConns == 0 {
			s.MaxConns = 10
		}
		if s.TimeoutSecs == 0 {
			s.TimeoutSecs = 30
		}
		if s.PasswordEnv != "" {
			if pw := os.Getenv(s.PasswordEnv); pw != "" {
				s.DSN = injectPassword(s.DSN, pw)
			}
		}
		c.Sinks[name] = s
	}
}

func (c *Config) Validate() error {
	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink must be configured")
	}
	hasEnabled := false
	for name, s := range c.Sinks {
		if s.Enabled {
			hasEnabled = true
			if s.DSN == "" {
				return fmt.Errorf("sink %q is enabled but has no dsn", name)
			}
		}
	}
	if !hasEnabled {
		return fmt.Errorf("at least one sink must be enabled")
	}
	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.setDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// injectPassword is a placeholder for DSN password injection.
// For postgres DSNs it would replace the password portion; for HTTP URLs
// it could set basic auth. For now it returns dsn unchanged if no
// clear injection point exists.
func injectPassword(dsn, password string) string {
	// Simple approach: if DSN contains ":@" (empty password), fill it in
	// e.g. postgres://user:@host → postgres://user:password@host
	for i := 0; i < len(dsn)-1; i++ {
		if dsn[i] == ':' && dsn[i+1] == '@' {
			return dsn[:i+1] + password + dsn[i+1:]
		}
	}
	return dsn
}
