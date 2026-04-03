package config

import (
	"os"
	"testing"
)

func TestLoadValid(t *testing.T) {
	content := `
server:
  bind: "0.0.0.0:9999"
sinks:
  postgres:
    enabled: true
    dsn: "postgres://localhost/test"
`
	f := writeTempFile(t, content)
	cfg, err := Load(f)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if cfg.Server.Bind != "0.0.0.0:9999" {
		t.Errorf("expected bind=0.0.0.0:9999, got %s", cfg.Server.Bind)
	}
	// Check defaults.
	if cfg.Batcher.MaxSize != 5000 {
		t.Errorf("expected default max_size=5000, got %d", cfg.Batcher.MaxSize)
	}
	pg := cfg.Sinks["postgres"]
	if pg.BulkSize != 5000 {
		t.Errorf("expected default bulk_size=5000, got %d", pg.BulkSize)
	}
	if pg.MaxRetries != 3 {
		t.Errorf("expected default max_retries=3, got %d", pg.MaxRetries)
	}
}

func TestLoadNoSinks(t *testing.T) {
	content := `
server:
  bind: ":8080"
`
	f := writeTempFile(t, content)
	_, err := Load(f)
	if err == nil {
		t.Fatal("expected error for no sinks")
	}
}

func TestLoadNoEnabledSinks(t *testing.T) {
	content := `
sinks:
  postgres:
    enabled: false
    dsn: "postgres://localhost/test"
`
	f := writeTempFile(t, content)
	_, err := Load(f)
	if err == nil {
		t.Fatal("expected error for no enabled sinks")
	}
}

func TestLoadMissingDSN(t *testing.T) {
	content := `
sinks:
  postgres:
    enabled: true
`
	f := writeTempFile(t, content)
	_, err := Load(f)
	if err == nil {
		t.Fatal("expected error for missing DSN")
	}
}

func TestPasswordInjection(t *testing.T) {
	content := `
sinks:
  postgres:
    enabled: true
    dsn: "postgres://user:@localhost/db"
    password_env: "TEST_PG_PASS"
`
	os.Setenv("TEST_PG_PASS", "secret123")
	defer os.Unsetenv("TEST_PG_PASS")

	f := writeTempFile(t, content)
	cfg, err := Load(f)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	expected := "postgres://user:secret123@localhost/db"
	if cfg.Sinks["postgres"].DSN != expected {
		t.Errorf("expected DSN=%s, got %s", expected, cfg.Sinks["postgres"].DSN)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(content)
	f.Close()
	return f.Name()
}
