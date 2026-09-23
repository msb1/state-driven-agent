package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	const testKey = "STATE_DRIVEN_DOTENV_TEST_MODEL"
	oldValue, wasSet := os.LookupEnv(testKey)
	_ = os.Unsetenv(testKey)
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(testKey, oldValue)
		} else {
			_ = os.Unsetenv(testKey)
		}
	})
	if err := os.WriteFile(path, []byte("# comment\nOPENAI_BASE_URL=\"http://127.0.0.1:1234/v1\"\nexport "+testKey+"=qwen-local\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_BASE_URL", "already-set")
	if err := loadDotEnvFile(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("OPENAI_BASE_URL"); got != "already-set" {
		t.Fatalf("existing environment value was overwritten: %q", got)
	}
	if got := os.Getenv(testKey); got != "qwen-local" {
		t.Fatalf(".env value was not loaded: %q", got)
	}
}

func TestLoadDotEnvFileRejectsMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("OPENAI_BASE_URL\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadDotEnvFile(path); err == nil {
		t.Fatal("expected malformed .env line to fail")
	}
}
